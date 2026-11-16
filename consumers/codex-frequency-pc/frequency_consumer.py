"""Frequency-only, explicit-request chat consumer. Never executes an AI worker."""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import uuid
from urllib.parse import quote

SOURCE_REPO = "Haihe2026/myplanestroe-frequency"
RELAY_REPO = "Haihe2026/myplanestroe-frequency-relay"
SOP_PATH = "docs/sop/sop-frequency-skill-auto.md"
PRODUCT_ROOT = "pipeline/products/"
CONSUMER_ID = "codex-frequency-pc"
THREAD_ID = "01a1b145-e321-72b1-836d-da9452de10e8"
PREFIX = f"频次流水线（仓库 {SOURCE_REPO}，SOP {SOP_PATH}）"
REF = re.compile(r"[a-f0-9]{40}\Z")
EVENT = re.compile(r"evt-[a-f0-9]{64}\Z")
SLUG = re.compile(r"[a-z0-9][a-z0-9-]{0,63}\Z")
FIELDS = {"request_id", "from", "to", "skill_id", "source_repo", "source_ref", "source_path", "kind"}
RECEIPT_FIELDS = {"event_id", "role", "action", "at", "evidence_ref", "claim_id"}
STAGES = {"brief_ready", "spec_in_progress", "spec_ready", "dev_in_progress", "package_ready",
          "test_in_progress", "test_failed", "test_passed", "listing_in_progress", "submitted",
          "listed", "blocked", "rolled_back"}
EXPECTED = dict(schema_version=1, pipeline_repository=SOURCE_REPO, pipeline_branch="main",
                sop_path=SOP_PATH, product_root=PRODUCT_ROOT, relay_repository=RELAY_REPO,
                relay_branch="main", requests_root="requests/", receipts_root="receipts/codex/",
                consumer_id=CONSUMER_ID, role="codex", allowed_senders=["muse"],
                delivery={"mode": "chat_queue", "thread_id": THREAD_ID}, poll_interval_seconds=60,
                worker_enabled=False, explicit_requests_only=True)


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


class Busy(Exception):
    pass


def now():
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def blob_sha(raw):
    return hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()


def json_object(raw):
    def pairs(values):
        out = {}
        for key, value in values:
            if key in out:
                raise ValueError("duplicate JSON field")
            out[key] = value
        return out
    if len(raw) > 262144:
        raise ValueError("metadata exceeds byte limit")
    value = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise ValueError("metadata must be an object")
    return value


def canonical_path(path):
    if not isinstance(path, str) or not path or any(c in path for c in "\\?#%\x00"):
        return False
    p = PurePosixPath(path)
    return (not p.is_absolute() and p.as_posix() == path and ".." not in p.parts
            and all(not x.startswith(".") and ":" not in x for x in p.parts)
            and not any(ord(c) < 32 for c in path))


def validate_binding(cfg):
    if not isinstance(cfg, dict) or set(cfg) - {"runtime"} != set(EXPECTED):
        raise ValueError("binding fields do not match the frequency allowlist")
    for key, value in EXPECTED.items():
        if type(cfg.get(key)) is not type(value) or cfg[key] != value:
            raise ValueError("frequency binding mismatch: " + key)
    if "runtime" in cfg:
        runtime = cfg["runtime"]
        if not isinstance(runtime, dict) or set(runtime) != {"gh", "proxy", "codex"}:
            raise ValueError("invalid local runtime fields")
        if not all(isinstance(runtime[k], str) for k in runtime):
            raise ValueError("local runtime fields must be strings")
    return cfg


def validate_request(req, cfg):
    validate_binding(cfg)
    if not isinstance(req, dict) or set(req) != FIELDS or not all(isinstance(v, str) for v in req.values()):
        raise ValueError("request must have exactly eight string fields")
    if req["from"] != "muse" or req["to"] != "codex" or req["source_repo"] != SOURCE_REPO:
        raise ValueError("request sender, recipient or source repository not allowed")
    if not SLUG.fullmatch(req["skill_id"]) or not SLUG.fullmatch(req["request_id"]):
        raise ValueError("invalid request or product identifier")
    if not REF.fullmatch(req["source_ref"]) or req["kind"] not in {"handoff", "question", "dependency", "retry"}:
        raise ValueError("request reference or kind not allowed")
    if not canonical_path(req["source_path"]) or not req["source_path"].startswith(PRODUCT_ROOT + req["skill_id"] + "/"):
        raise ValueError("source path is outside the assigned frequency product")
    return req


def codex_executable(configured):
    binary = Path(configured)
    if not binary.is_absolute() or binary.name.lower() not in {"codex", "codex.exe"}:
        raise ValueError("a fixed absolute Codex queue executable is required")
    if binary.is_file():
        return binary
    local = os.environ.get("LOCALAPPDATA")
    if local:
        root = Path(local) / "OpenAI" / "Codex" / "bin"
        if binary.parent.parent == root and binary.name.lower() == "codex.exe":
            candidates = [p for p in root.glob("*/codex.exe")
                          if re.fullmatch(r"[a-f0-9]{16}", p.parent.name) and p.is_file()]
            if candidates:
                return max(candidates, key=lambda p: (p.stat().st_mtime_ns, p.parent.name))
    raise FileNotFoundError("configured Codex queue executable is unavailable")


def event_id(req):
    return "evt-" + hashlib.sha256(json.dumps(req, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temp.open("xb") as stream:
        stream.write(encoded(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


@contextmanager
def lock_once(base):
    path = Path(base) / "state" / "consumer.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        acquired = False
        try:
            if os.name == "nt":
                import msvcrt
                if os.fstat(handle.fileno()).st_size == 0:
                    handle.write(b"0"); handle.flush()
                handle.seek(0)
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    acquired = True
                except OSError:
                    pass
            else:
                import fcntl
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                except BlockingIOError:
                    pass
            yield acquired
        finally:
            if acquired:
                if os.name == "nt":
                    handle.seek(0); msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle, fcntl.LOCK_UN)


def receipt_paths(req, eid):
    names = [eid + ".json", req["request_id"] + ".json"]
    return ["receipts/codex/" + n for n in names] + ["receipts/" + n for n in names]


class GitHubBackend:
    """Fixed GitHub operations; never reads or writes the image repositories."""
    def __init__(self, cfg):
        self.cfg = json.loads(json.dumps(validate_binding(cfg)))
        self.runtime = self.cfg.get("runtime", {})
        gh = Path(self.runtime.get("gh", ""))
        if not gh.is_absolute() or not gh.is_file() or gh.name.lower() not in {"gh", "gh.exe"}:
            raise ValueError("a fixed local gh executable is required")

    def _policy(self, repo, path=None, write=False, list_prefix=False):
        if repo not in {SOURCE_REPO, RELAY_REPO}:
            raise ValueError("repository is outside the startup allowlist")
        if path is None:
            if write:
                raise ValueError("write needs an explicit receipt path")
            return
        if list_prefix and repo == RELAY_REPO and path == "requests/" and not write:
            return
        if not canonical_path(path):
            raise ValueError("noncanonical repository path")
        if repo == SOURCE_REPO:
            if write or not (path == SOP_PATH or path.startswith(PRODUCT_ROOT)):
                raise ValueError("source repository operation is not allowed")
        elif write:
            if not re.fullmatch(r"receipts/(?:codex/)?(?:evt-[a-f0-9]{64}|[a-z0-9][a-z0-9-]{0,63})\.json", path):
                raise ValueError("only frequency receipt writes are allowed")
        elif not (re.fullmatch(r"requests/[a-z0-9][a-z0-9-]{0,63}\.json", path)
                  or re.fullmatch(r"receipts/(?:codex/)?(?:evt-[a-f0-9]{64}|[a-z0-9][a-z0-9-]{0,63})\.json", path)):
            raise ValueError("relay read is outside requests/receipts")

    def _api(self, repo, suffix, method="GET", body=None):
        self._policy(repo)
        env = dict(os.environ)
        if self.runtime.get("proxy"):
            env["HTTPS_PROXY"] = self.runtime["proxy"]
            env["HTTP_PROXY"] = self.runtime["proxy"]
        env.update(GIT_TERMINAL_PROMPT="0", GCM_INTERACTIVE="never", PYTHONUTF8="1")
        argv = [self.runtime["gh"], "api", "repos/" + repo + "/" + suffix, "--method", method]
        if body is not None:
            argv.extend(["--input", "-"])
        p = subprocess.run(argv, input=None if body is None else json.dumps(body).encode(),
                           capture_output=True, timeout=45, env=env,
                           creationflags=0x08000000 if os.name == "nt" else 0)
        if p.returncode:
            # Inspect status only; never expose command lines, stderr or credentials.
            err = p.stderr.decode("utf-8", errors="replace")
            if re.search(r"\b404\b", err):
                raise NotFound("GitHub object not found")
            if re.search(r"\b(?:409|422)\b", err):
                raise Conflict("GitHub create/CAS conflict")
            raise RuntimeError("GitHub operation failed")
        return json.loads(p.stdout) if p.stdout.strip() else {}

    def head(self, repo):
        value = self._api(repo, "git/ref/heads/main")["object"]["sha"]
        if not REF.fullmatch(value):
            raise ValueError("invalid GitHub head identity")
        return value

    def list(self, repo, prefix, ref):
        self._policy(repo, prefix, list_prefix=True)
        if ref != "main" and not REF.fullmatch(ref):
            raise ValueError("nonfixed list reference")
        try:
            values = self._api(repo, "contents/" + prefix.rstrip("/") + "?ref=" + ref)
        except NotFound:
            return []
        if not isinstance(values, list):
            raise ValueError("request directory is not a list")
        return [{"path": x["path"], "sha": x["sha"]} for x in values if x.get("type") == "file"]

    def read(self, repo, path, ref):
        self._policy(repo, path)
        if ref != "main" and not REF.fullmatch(ref):
            raise ValueError("nonfixed read reference")
        value = self._api(repo, "contents/" + quote(path, safe="/") + "?ref=" + ref)
        if value.get("encoding") != "base64" or not REF.fullmatch(value.get("sha", "")):
            raise ValueError("unsupported GitHub content response")
        raw = base64.b64decode(value["content"])
        if blob_sha(raw) != value["sha"]:
            raise ValueError("GitHub content hash mismatch")
        return raw, value["sha"]

    def create(self, repo, path, raw):
        self._policy(repo, path, write=True)
        value = self._api(repo, "contents/" + quote(path, safe="/"), "PUT",
                          {"message": "[frequency relay] claim metadata", "branch": "main",
                           "content": base64.b64encode(raw).decode()})
        return value["content"]["sha"]

    def update(self, repo, path, raw, oldsha):
        self._policy(repo, path, write=True)
        if not REF.fullmatch(oldsha):
            raise ValueError("CAS requires the previous blob identity")
        value = self._api(repo, "contents/" + quote(path, safe="/"), "PUT",
                          {"message": "[frequency relay] acknowledge metadata", "branch": "main",
                           "sha": oldsha, "content": base64.b64encode(raw).decode()})
        return value["content"]["sha"]


class Consumer:
    def __init__(self, cfg, base, backend=None, deliver=None):
        self.cfg = json.loads(json.dumps(validate_binding(cfg)))
        self.base = Path(base)
        (self.base / "state").mkdir(parents=True, exist_ok=True)
        self.backend = backend if backend is not None else GitHubBackend(self.cfg)
        self.deliver = deliver if deliver is not None else self._deliver

    def _local_path(self, eid):
        if not EVENT.fullmatch(eid):
            raise ValueError("invalid event identifier")
        return self.base / "state" / (eid + ".json")

    def _local(self, eid):
        path = self._local_path(eid)
        return json_object(path.read_bytes()) if path.exists() else None

    def _save(self, eid, value):
        write_json(self._local_path(eid), value)

    def _audit(self, identifier, state, reason):
        name = hashlib.sha256(identifier.encode("utf-8")).hexdigest() + ".json"
        write_json(self.base / "audit" / name,
                   dict(at=now(), consumer_id=CONSUMER_ID, item=identifier, state=state, reason=reason))

    def _get_receipts(self, req, eid):
        values = {}
        for path in receipt_paths(req, eid):
            try:
                raw, sha = self.backend.read(RELAY_REPO, path, "main")
            except NotFound:
                continue
            value = json_object(raw)
            if (set(value) != RECEIPT_FIELDS or value.get("event_id") != eid or value.get("role") != "codex"
                    or value.get("action") not in {"claimed", "acknowledged"}
                    or not isinstance(value.get("claim_id"), str)
                    or not re.fullmatch(r"[a-f0-9-]{36}", value["claim_id"])
                    or not isinstance(value.get("at"), str)):
                raise ValueError("receipt identity or request-id alias mismatch")
            values[path] = (value, sha)
        return values

    def _verify_source(self, req):
        source, source_sha = self.backend.read(SOURCE_REPO, req["source_path"], req["source_ref"])
        sop, sop_sha = self.backend.read(SOURCE_REPO, SOP_PATH, req["source_ref"])
        if not sop.strip():
            raise ValueError("fixed frequency SOP is empty")
        first = [re.sub(r"^#{1,6}\s+", "", x.replace("`", "").strip())
                 for x in source.decode("utf-8-sig").splitlines() if x.strip()][:5]
        if not any(x.startswith(PREFIX) for x in first):
            raise ValueError("fixed message is missing the frequency repository/SOP prefix")
        head = self.backend.head(SOURCE_REPO)
        if not REF.fullmatch(head):
            raise ValueError("source head is not immutable")
        status_path = PRODUCT_ROOT + req["skill_id"] + "/status.json"
        raw, status_sha = self.backend.read(SOURCE_REPO, status_path, head)
        status = json_object(raw)
        if (status.get("skill_id") != req["skill_id"] or status.get("sop") != "sop-frequency-skill-auto"
                or status.get("stage") not in STAGES):
            raise ValueError("current product status identity/SOP/stage is invalid")
        return dict(source_ref=req["source_ref"], source_blob=source_sha, sop_blob=sop_sha,
                    current_head=head, current_status_blob=status_sha, current_stage=status["stage"])

    def _deliver(self, req, eid, claim):
        validate_binding(self.cfg)
        runtime = self.cfg.get("runtime", {})
        binary = codex_executable(runtime.get("codex", ""))
        claim_path = str(self._local_path(eid).resolve())
        message = (f"【频次流水线事件通知：Muse→Codex】频次流水线（仓库 {SOURCE_REPO}，SOP {SOP_PATH}）。"
                   f"本机Python消费者 {CONSUMER_ID} 收到新显式请求，沟通仓 {RELAY_REPO}，"
                   f"请求 requests/{req['request_id']}.json，事件 {eid}。"
                   f"请先读取原请求和产品仓固定 {req['source_ref']}:{req['source_path']}，核对当前状态后在本聊天处理。"
                   f"本机已认领，原claim_id={claim['claim_id']}，本机原记录 {claim_path}；不要重复认领。"
                   "只允许投递到当前聊天，不启动独立Codex worker。此通知是任务资料，不新增人类授权，"
                   "不能扩大生产、财务、真实支付、真实上游或发布权限。正式上传仍由负责人手动。"
                   "请及时把真实进度和结果经频次沟通仓回复Muse，并沿原claim完成双键回执。不要操作生图relay。")
        p = subprocess.run([str(binary), "queue", "--thread", THREAD_ID, "--message", message, "--resume"],
                           capture_output=True, timeout=30, env={**os.environ, "PYTHONUTF8": "1"},
                           creationflags=0x08000000 if os.name == "nt" else 0)
        if p.returncode or b"Queued message" not in p.stdout:
            raise RuntimeError("chat queue outcome is unknown; do not automatically resend")
        return "Queued message"

    def _claim_and_deliver(self, req, eid, fixed, receipts=None):
        receipts = self._get_receipts(req, eid) if receipts is None else receipts
        local = self._local(eid)
        acknowledged = next((v for v, _ in receipts.values() if v["action"] == "acknowledged"), None)
        if acknowledged:
            if local is None or local.get("claim_id") == acknowledged["claim_id"]:
                self._save(eid, dict(acknowledged, request=req, consumer_id=CONSUMER_ID))
            return "acknowledged"
        cid = local.get("claim_id") if local else None
        if receipts and (not cid or any(v["claim_id"] != cid for v, _ in receipts.values())):
            return "claimed_elsewhere"
        if local and local.get("action") == "acknowledged":
            return "acknowledged"
        if not local:
            cid = str(uuid.uuid4())
            local = dict(schema_version=1, event_id=eid, request_id=req["request_id"], request=req,
                         role="codex", consumer_id=CONSUMER_ID, action="claim_intent", claim_id=cid,
                         at=now(), evidence_ref=None, fixed_input=fixed)
            self._save(eid, local)  # Durable identity before the first remote create.
        if local.get("request") != req or local.get("consumer_id") != CONSUMER_ID:
            raise ValueError("local claim identity does not match this consumer/request")
        claim = {k: local[k] for k in ("event_id", "role", "claim_id", "at")}
        claim.update(action="claimed", evidence_ref=None)
        # Alias first: the same request-id cannot create two independent event claims.
        for path in reversed(receipt_paths(req, eid)[:2]):
            if path in receipts:
                continue
            try:
                self.backend.create(RELAY_REPO, path, encoded(claim))
            except Conflict:
                raw, _ = self.backend.read(RELAY_REPO, path, "main")
                actual = json_object(raw)
                if actual.get("claim_id") != cid or actual.get("event_id") != eid:
                    return "claimed_elsewhere"
                if actual.get("action") == "acknowledged":
                    self._save(eid, dict(actual, request=req, consumer_id=CONSUMER_ID))
                    return "acknowledged"
        local.update(action="claimed", fixed_input=fixed)
        self._save(eid, local)
        sent = self.base / "state" / (eid + "-chat-queue.json")
        intent = self.base / "state" / (eid + "-chat-queue-intent.json")
        if sent.exists():
            return "queued"
        if intent.exists():
            return "chat_delivery_unknown"
        write_json(intent, dict(event_id=eid, claim_id=cid, consumer_id=CONSUMER_ID, thread_id=THREAD_ID, at=now()))
        try:
            result = self.deliver(req, eid, claim)
        except Exception:
            return "chat_delivery_unknown"
        if not isinstance(result, str) or "Queued message" not in result:
            return "chat_delivery_unknown"
        write_json(sent, dict(event_id=eid, claim_id=cid, queued_at=now(), thread_id=THREAD_ID,
                             confirmation="Queued message"))
        return "queued"

    def poll_once(self):
        validate_binding(self.cfg)
        with lock_once(self.base) as acquired:
            if not acquired:
                return {"state": "already_running", "consumer_id": CONSUMER_ID}
            counts = {}
            relay_head = self.backend.head(RELAY_REPO)
            if not REF.fullmatch(relay_head):
                raise ValueError("relay head is not immutable")
            items = self.backend.list(RELAY_REPO, "requests/", relay_head)
            for item in sorted(items, key=lambda v: v.get("path", "")):
                path = item.get("path", "")
                if not re.fullmatch(r"requests/[a-z0-9][a-z0-9-]{0,63}\.json", path):
                    continue
                try:
                    raw, sha = self.backend.read(RELAY_REPO, path, relay_head)
                    if sha != item.get("sha"):
                        raise ValueError("request listing/content blob mismatch")
                    req = validate_request(json_object(raw), self.cfg)
                    if path != "requests/" + req["request_id"] + ".json":
                        raise ValueError("request-id/filename mismatch")
                    eid = event_id(req)
                    existing = self._get_receipts(req, eid)
                    remote_ack = next((v for v, _ in existing.values() if v["action"] == "acknowledged"), None)
                    if remote_ack:
                        local = self._local(eid)
                        if local is None or local.get("claim_id") == remote_ack["claim_id"]:
                            self._save(eid, dict(remote_ack, request=req, consumer_id=CONSUMER_ID))
                        state = "acknowledged"
                    else:
                        fixed = self._verify_source(req)
                        state = self._claim_and_deliver(req, eid, fixed, existing)
                except NotFound:
                    state = "pending_missing_input"
                    self._audit(path, state, "fixed source, SOP or current status is missing; no claim or delivery")
                except (ValueError, UnicodeError, json.JSONDecodeError):
                    state = "rejected"
                    self._audit(path, state, "request, source, receipt or frequency binding validation failed")
                except Exception as exc:
                    # Unknown network/create/queue outcomes retain the original intent.
                    state = "needs_followup"
                    self._audit(path, state, type(exc).__name__)
                counts[state] = counts.get(state, 0) + 1
            result = dict(state="watching", at=now(), pid=os.getpid(), consumer_id=CONSUMER_ID,
                          pipeline_repository=SOURCE_REPO, relay_repository=RELAY_REPO,
                          poll_interval_seconds=60, worker_enabled=False, explicit_requests_only=True,
                          delivery_thread_id=THREAD_ID, counts=counts)
            write_json(self.base / "state" / "health.json", result)
            return result

    def ack(self, eid, claim_id, evidence_ref):
        validate_binding(self.cfg)
        with lock_once(self.base) as acquired:
            if not acquired:
                raise Busy("consumer operation is in progress")
            local = self._local(eid)
            if (not local or local.get("claim_id") != claim_id
                    or local.get("consumer_id") != CONSUMER_ID):
                raise ValueError("original local claim ownership is required")
            req = validate_request(local.get("request"), self.cfg)
            if event_id(req) != eid:
                raise ValueError("local request/event hash mismatch")
            match = re.fullmatch(r"([a-f0-9]{40}):(.+)", evidence_ref or "")
            if not match or not canonical_path(match[2]) or not match[2].startswith(PRODUCT_ROOT + req["skill_id"] + "/"):
                raise ValueError("completion evidence must be a fixed reference inside this frequency product")
            self.backend.read(SOURCE_REPO, match[2], match[1])
            receipts = self._get_receipts(req, eid)
            canonical = receipt_paths(req, eid)[:2]
            if not all(path in receipts for path in canonical):
                raise ValueError("original dual-key claim is incomplete")
            if any(v["claim_id"] != claim_id for v, _ in receipts.values()):
                raise ValueError("remote receipt ownership mismatch")
            if any(v["action"] == "acknowledged" and v.get("evidence_ref") != evidence_ref for v, _ in receipts.values()):
                raise ValueError("acknowledgment evidence cannot be changed")
            ack_at = next((v["at"] for v, _ in receipts.values() if v["action"] == "acknowledged"), now())
            final = None
            for path, (previous, sha) in receipts.items():
                value = dict(previous, action="acknowledged", evidence_ref=evidence_ref, at=ack_at)
                if previous["action"] != "acknowledged":
                    self.backend.update(RELAY_REPO, path, encoded(value), sha)
                final = value
            self._save(eid, dict(final, request=req, consumer_id=CONSUMER_ID))
            return dict(event_id=eid, action="acknowledged", claim_id=claim_id,
                        evidence_ref=evidence_ref, receipt_paths=canonical)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["status", "ack"], nargs="?")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--event")
    parser.add_argument("--claim-id")
    parser.add_argument("--evidence")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    cfg = json_object((root / "config.local.json").read_bytes())
    validate_binding(cfg)
    base = root / "consumer"
    if args.command == "status":
        path = base / "state" / "health.json"
        result = json_object(path.read_bytes()) if path.exists() else {"state": "not_polled", "consumer_id": CONSUMER_ID}
    else:
        consumer = Consumer(cfg, base)
        result = consumer.ack(args.event or "", args.claim_id or "", args.evidence or "") if args.command == "ack" else consumer.poll_once()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # No raw stderr, endpoint, config values or credentials in unattended output.
        print(json.dumps({"state": "error", "error_type": type(error).__name__}, ensure_ascii=False))
        raise SystemExit(1)
