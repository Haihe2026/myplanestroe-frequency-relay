"""Isolated frequency request-ingress regressions; no real network, queue or product execution.

Natural-language message layout is not an authorization boundary. Fixed request,
repository, reference, assigned product, SOP and status checks remain boundaries.
Rejection diagnostics are not claims and are retained once in the bound relay.
"""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import frequency_consumer as F

REAL_POPEN = subprocess.Popen

SOURCE = 'Haihe2026/myplanestroe-frequency'
RELAY = 'Haihe2026/myplanestroe-frequency-relay'
SOP = 'docs/sop/sop-frequency-skill-auto.md'
THREAD = '01a1b145-e321-72b1-836d-da9452de10e8'
SKILL = 'cross-border-keyword-ranking'
PRODUCT = 'pipeline/products/' + SKILL + '/'
REF = 'a' * 40
CURRENT = 'c' * 40
RELAY_REF = 'd' * 40
EVIDENCE_REF = 'e' * 40
PREFIX = '频次流水线（仓库 Haihe2026/myplanestroe-frequency，SOP docs/sop/sop-frequency-skill-auto.md）'

# Exact fixed message bytes from the two real requests; all backends, status and delivery remain synthetic.
FIXED_MESSAGES = json.loads(r'''[
  {
    "request": {
      "from": "muse",
      "kind": "handoff",
      "request_id": "req-muse-to-codex-cross-border-keyword-ranking-spec-20261116",
      "skill_id": "cross-border-keyword-ranking",
      "source_path": "pipeline/products/cross-border-keyword-ranking/msg-from-muse-20261116-cross-border-keyword-ranking-brief.md",
      "source_ref": "9dfa8c99357c6b4bf31c0bf28bf7cb5019138fb1",
      "source_repo": "Haihe2026/myplanestroe-frequency",
      "to": "codex"
    },
    "message_base64": "IyDkuqTmjqXvvJrpopHmrKHnur/pppbljZXpgInlk4HljZUg4oaSIENvZGV4IOWGmeinhOagvAoKLSDkuqflk4HvvJrot6jlooPlhbPplK7or43mjpLlkI3liqnmiYvvvIhjcm9zcy1ib3JkZXIta2V5d29yZC1yYW5raW5n77yJ77yM6aKR5qyh57q/6aaW5Y2V77yI5rWL6K+V5Y2V77yJ77yMUDEKLSDpgInlk4HljZXvvJpwaXBlbGluZS9wcm9kdWN0cy9jcm9zcy1ib3JkZXIta2V5d29yZC1yYW5raW5nLzAwLWJyaWVmLm1kCi0g6LCD56CU77yacGlwZWxpbmUvcmVzZWFyY2gvMjAyNi0xMS0xNi1yb3VuZC1mcmVxLTAxLm1kCi0g54q25oCB77yaYnJpZWZfcmVhZHnvvIhzdGF0dXMuanNvbiDkuI7mnKzkuqTmjqXlkIzmibnlu7rnq4vvvIkKLSBTT1DvvJpkb2NzL3NvcC9zb3AtZnJlcXVlbmN5LXNraWxsLWF1dG8ubWTvvIjmtYvor5XljZXnjrDniYjojYnnqL/vvJvlrprnqL/liY3mjInnjrDniYjotbDvvIkKLSDmtYHmsLTnur/ku5PvvJpIYWloZTIwMjYvbXlwbGFuZXN0cm9lLWZyZXF1ZW5jee+8iOWPquWKqOi/meS4quS7k++8m+e7neS4jeWKqOeUn+WbvuS7kyBIYWloZTIwMjYvbXlwbGFuZXN0cm9l77yJCi0g5rKf6YCa5LuT77yaSGFpaGUyMDI2L215cGxhbmVzdHJvZS1mcmVxdWVuY3ktcmVsYXnvvIjlj6rliqjov5nkuKrku5PvvJvnu53kuI3liqjnlJ/lm74gcmVsYXnvvIkKCiMjIOS7u+WKoQoK5oyJIFNPUCBCLjIg5YaZ5byA5Y+R5paH5qGj77yaMDEtZGV2LXNwZWMubWTvvIjmjIkgQS43IOeahCBBMSDmuIXljZXpgJDoioLlhpnvvIznq6DoioLpobrluo/kuI4gQTEg5riF5Y2V5LiA6Ie077yJ5LiOIDAxLWRldi1zcGVjLmpzb27vvIjlj6rmlL7mnLrlmajopoHnlKjnmoTmkZjopoHvvIzmjIkgQi4yIOaooeadv++8jOWQqyBza2lsbF9pZOOAgWRpc3BsYXlfbmFtZeOAgXZlcnNpb27jgIFsYW5l44CBY2hhbm5lbHPjgIFiaWxsaW5n44CBc3VwcGxpZXLjgIFtYXJnaW7jgIFzY2VuZXPjgIFzYW1wbGVz44CBdGltZW91dHPjgIFlbmRwb2ludHPjgIFmdW5uZWxfc3RlcHPjgIFzaWJsaW5nX3NraWxsc+OAgXRlc3Rz44CBZGVjaXNpb25z77yJ44CCCgojIyDlhpnkuYvliY3lv4XlgZoKCi0g5Zyo57q/6YeN6K+7IFdvcmtCdWRkeSDlrpjmlrnmlofmoaPkuInpobXvvIhvcGVuLndvcmtidWRkeS5jbi9kb2NzL3NraWxs44CBL2RvY3MvZXhwZXJ044CB5bmz5Y+w5qaC6L+w77yJ77yM5YaN5YaZ5byA5Y+R5paH5qGj77yb5LuT5bqT6YeM55qE5a6Y5pa55paH5qGj5b+r54Wn5Y+q5L2c6KGl5YWF77yM5LiN6IO95b2T5L2c5bey5a6M5oiQ5Zyo57q/6K+75Y+W55qE6K+B5o2u44CC6K+75LiN5Yiw5bCx5oum5YGc77yIT0ZGSUNJQUxfRE9DU19VTlJFQURBQkxF77yJ77yM5Y+RIHJlbGF5IOWbnuaKpe+8jOS4jeehrOWGmeOAggotIOWFiOaLieacgOaWsOS7k+W6k+WGjeiupOmihu+8muaKiiBzdGF0dXMuanNvbiDnmoQgc3RhZ2Ug5pS55oiQIHNwZWNfaW5fcHJvZ3Jlc3PjgIFvd25lciDlhpkgY29kZXjjgIFjbGFpbWVkX2F0IOWGmeW9k+WJjeaXtumXtO+8jOaPkOS6pOaOqOmAgeaIkOWKn+WQjuaJjeW8gOWni+W5sua0u+OAggoKIyMg5Zu65a6a6KeE5YiZ77yI5YaZ6L+b6KeE5qC877yM5LiN5b6X6L+d5Y+N77yJCgotIOaUr+S7mOmhteWbuuWumiBodHRwczovL215cGxhbmVzdG9yZS5jb20vbWVtYmVyc2hpcC9saXN0aW5nL++8jOmAkOWtl+aguOWvue+8jOe7neS4jeiDveWGmeaIkOeUn+Wbvue6v+eahCBodHRwczovL215cGxhbmVzdG9yZS5jb20vbWVtYmVyc2hpcC/jgIIKLSDorqHotLnvvJrkuJrliqHljZXku7cgMC4zMCDlhYMv5qyh77yM5oyJ5a6e6ZmF5pyJ5pWI57uT5p6c5pWw5omj77yM5rKh5pWw5o2u55qE5LiN5omj77yM5LiA5p2h5pyJ5pWI57uT5p6c6YO95rKh5pyJ5bCx5LiN5omj6LS577yb5paw57q/57uf5LiAIDAg5qyh5YWN6LS56K+V55So44CC5bey5a6a5LqL6aG55oyJIFNPUCBBLjUg5aSE55CG77yM5LiN5YaZ5YWl6KeE5qC85q2j5paH5a6a5Lu35q6144CCCi0g5ZCI6KeE77ya5LiN5o6l5Y2W5a625ZCO5Y+w44CB5LiN54is6ZyA55m75b2V55qE5Y2W5a625pWw5o2u44CB5LiN5Luj5Yi35o6S5ZCN44CB5LiN5om/6K+65o6S5ZCN5Zue5Y2H44CB5LiN5pS26ZuG5bqX6ZO66LSm5Y+35a+G56CB6K6i5Y2V562J5Liq5Lq65L+h5oGv44CCCi0g55m95qCH77ya55So5oi35Y+v6KeB55qE5paH5qGI44CB5Zu+54mHIFVSTOOAgeaKpemUmeOAgeawtOWNsOS4jeW+l+WHuueOsOS4iua4uOaooeWei+WQjS/mnI3liqHllYblkI0v5Z+f5ZCNL+WTgeeJjOOAggotIOS4u+mimCBsb2dv77ya5YaZ5byA5Y+R5paH5qGj5pe25LiA5bm255So5LiK5ri46LWE5rqQ55Sf5oiQ77yM5L+d5a2Y5YiwIDAyLXBhY2thZ2UvZGlzdC9sb2dvLnBuZ++8jOiusCBwcm92ZW5hbmNlIOS4jiBhdWRpdO+8m3NwZWNfcmVhZHkg5Y2z6KGo56S6IGxvZ28g5bey5bCx5L2N44CCCi0g5LiN57yW6YCg77yaV29ya0J1ZGR5IOermeWGheaVsOWtl+WboOeZu+W9leWimeacqumqjOWIsO+8jOWGmeOAjOacqumqjOWIsOOAje+8jOS4jee8luS4i+i9vemHj+S4juivhOWIhu+8m1NraWxsSHViIOermeWGheaVsOWtl+S7pemAieWTgeWNleS4uuWHhuOAggotIOinhOagvOmAgOWbnuS4iumZkCAxIOasoe+8m21heF9yb3VuZHMg5oyJ5bCd6K+V5pWw6K6h77yI5q+P5qyh5bCd6K+V6K6hIDEg5qyh77yJ44CCCgojIyDmoLjlv4Plt67lgLzvvIjlhpnop4TmoLzml7blrojkvY/vvIkKCi0g5Y+q5YGa5aSa5YWz6ZSu6K+Nw5flpJrml6XmjpLlkI3miJDooajvvJrnlKjmiLfnspjotLTlhbPplK7or43kuI7mjpLlkI3orrDlvZXvvIjmiJbnlLHlkI7nq6/lj5bmlbDvvInihpIg5LiA5byg5aSa5YWz6ZSu6K+Nw5flpJrml6XnmoTmjpLlkI3lj5jljJbooajvvIzmjonlh7rpppbpobXkuI7ov57nu63kuIvot4zmoIfnuqLjgIIKLSDkuI3lgZrlhbPplK7or43mjJbmjpgv6YCJ6K+N77yM5LiN5YGa5bm/5ZGK5oqV5pS+5LyY5YyW77yM5LiN5YGaIExpc3Rpbmcg5pS55YaZ77yb5LiN5YGa56ue5ZOB5YWo5aWX55uR5o6n77yI5Lu35qC8L+S/g+mUgC/mlrDlk4HvvInvvIzlj6rlgZrlhbPplK7or43mjpLlkI3ot5/ouKrvvJvpppbniYjkuI3lgZrkuIvot4zoh6rliqjmj5DphpLjgIIKLSDmlbDmja7mnaXmupDvvJrnlKjmiLfnspjotLTvvIvlkI7nq6/oh6rot5HosLfmrYzotovlir8v5oqT5Lqa6ams6YCK562J5byA5pS+5bmz5Y+w5pWw5o2u5pW055CG5ZCO5L6bIHNraWxsIOiwg+eUqO+8m+WQjuerr+WFseS6q+S4jumalOemu+S4jeWFsyBza2lsbCDlvIDlj5HnmoTkuovjgIIKCiMjIOWujOaIkOWQjgoK5YaZ5a6M6KeE5qC85ZCO5YGaIEExIOiHquajgO+8jOmAmui/h+WQjue9riBzcGVjX3JlYWR577yI5pu05pawIHVwZGF0ZWRfYXTvvInvvIxsb2cubWQg6L+95Yqg5LiA6KGM77yM5Y+RIHJlbGF5IOS6pOaOpee7mSBtdXNl77yIa2luZD1oYW5kb2Zm77yMc291cmNlIOaMh+W8gOWPkeaWh+aho+aPkOS6pO+8ie+8jOS6pCBNdXNlIOWkjeajgOOAgkExIOS4jemAmui/h+aIlumcgOaLpuWBnOaXtu+8jOaMiSBTT1AgQS4zL0EuNCDnva4gYmxvY2tlZO+8iOWGmea4hSByZWFzb25fY29kZeOAgWRldGFpbOOAgW5lZWRz44CBcmVzdW1lX3Rv77yJ77yM5bm25ZyoIEJMT0NLRUQubWQg6L+95Yqg5LiA6KGM77yM5Y+RIHJlbGF5IOWbnuaKpeOAggo=",
    "message_sha256": "70b609f367649465b0523d07bcd39466bed2a4d46a8174458f6478a127440270"
  },
  {
    "request": {
      "from": "muse",
      "kind": "handoff",
      "request_id": "req-muse-to-codex-cross-border-keyword-ranking-status-20261117",
      "skill_id": "cross-border-keyword-ranking",
      "source_path": "pipeline/products/cross-border-keyword-ranking/msg-from-muse-20261117-cross-border-keyword-ranking-status-check.md",
      "source_ref": "5b6745fe6b35335bbc33ce24614dd0a48dda83ab",
      "source_repo": "Haihe2026/myplanestroe-frequency",
      "to": "codex"
    },
    "message_base64": "IyDkuqTmjqXvvJrnoa7orqTpopHmrKHmtojotLnogIXnirbmgIHvvIjpppbljZXmnKrorqTpoobvvIkKCi0g5Lqn5ZOB77ya6Leo5aKD5YWz6ZSu6K+N5o6S5ZCN5Yqp5omL77yIY3Jvc3MtYm9yZGVyLWtleXdvcmQtcmFua2luZ++8ie+8jOmikeasoee6v+mmluWNle+8iOa1i+ivleWNle+8iQotIOiDjOaZr++8mk11c2Ug5bey5LqOIDIwMjYtMTEtMTcgMDA6MzYg5rS+6aaW5Y2V5YaZ6KeE5qC86K+35rGC77yIcmVxdWVzdF9pZCByZXEtbXVzZS10by1jb2RleC1jcm9zcy1ib3JkZXIta2V5d29yZC1yYW5raW5nLXNwZWMtMjAyNjExMTbvvIzkuqTmjqXkv6Hmj5DkuqQgOWRmYThjOTnvvInvvJvliLAgMDE6MDHvvIxzdGF0dXMuanNvbiDku43mmK8gYnJpZWZfcmVhZHnjgIFvd25lciDkuLrnqbrvvIzmsp/pgJrku5PkuZ/mnKrop4EgQ29kZXgg55qE6K6k6aKG44CB5Zue5omn5oiW6L+b5bqm44CCCi0g5raI6LS56ICF55m76K6w77yaY29kZXgtZnJlcXVlbmN5LXBj77yMMjAyNi0xMS0xNiAyMjoyNSDnmbvorrDvvIznirbmgIEgcmVhZHlfd2FpdGluZ19tdXNlX3JlcXVlc3TvvIw2MCDnp5Lova7or6LjgIIKCiMjIOivt+ehruiupAoKMS4g6aKR5qyh5raI6LS56ICF6L+b56iL546w5Zyo5piv5ZCm5Zyo6LeR77yI5pys5Zyw5py65Zmo77yJ77yb5aaC5rKh5Zyo6LeR77yM5oyJ55m76K6w5pe255qE5ZCv5Yqo5pa55byP6YeN5paw5ZCv5Yqo44CCCjIuIOaYr+WQpuaUtuWIsOS4iui/sOWGmeinhOagvOivt+axgu+8m+WmguacquaUtuWIsO+8jOivtOaYjuWPr+iDveeahOWOn+WboO+8iOi9ruivouebruagh+OAgeeZveWQjeWNleOAgee9kee7nOOAgeWHreaNru+8ieOAggozLiDpooTorqHkvZXml7blvIDlp4vlhpnop4TmoLzvvJvlpoLmmoLml7bml6Dms5XlvIDlp4vvvIzor7TmmI7ljaHngrnjgIIKCiMjIOWbnuWkjeaWueW8jwoK6YCa6L+H5rKf6YCa5LuT5Zue5LiA5Liq5Lqk5o6l57uZIG11c2XvvIhraW5kPWhhbmRvZmbvvIx0byBtdXNl77yJ77yM6ZmE5LiK6L+w56Gu6K6k57uT5p6c77yb5aaC6ZyA5oum5YGc77yM5oyJIFNPUCBBLjMvQS40IOe9riBibG9ja2VkIOW5tuWGmea4hSByZWFzb25fY29kZeOAgW5lZWRz44CBcmVzdW1lX3Rv44CCCgojIyDovrnnlYwKCuWPquWKqOS4pOmikeasoeS7k++8iEhhaWhlMjAyNi9teXBsYW5lc3Ryb2UtZnJlcXVlbmN544CBSGFpaGUyMDI2L215cGxhbmVzdHJvZS1mcmVxdWVuY3ktcmVsYXnvvInvvIznu53kuI3liqjnlJ/lm77ku5PkuI7nlJ/lm74gcmVsYXnvvJvkuI3mlLkgU09Q77yb5LiN5LiK5Lyg44CB5LiN5Y+R5biD44CCCg==",
    "message_sha256": "6c239e2bf46944579dc3ab6473b16bb7aad7ea30d26ffc3f9011fe0d62e00225"
  }
]''')


def raw(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode('utf-8')


def blob(value):
    return hashlib.sha1(b'blob ' + str(len(value)).encode() + b'\0' + value).hexdigest()


def config():
    return dict(schema_version=1, pipeline_repository=SOURCE, pipeline_branch='main',
                sop_path=SOP, product_root='pipeline/products/', relay_repository=RELAY,
                relay_branch='main', requests_root='requests/', receipts_root='receipts/codex/',
                consumer_id='codex-frequency-pc', role='codex', allowed_senders=['muse'],
                delivery=dict(mode='chat_queue', thread_id=THREAD), poll_interval_seconds=60,
                worker_enabled=False, explicit_requests_only=True,
                runtime=dict(gh=sys.executable, codex=sys.executable, proxy=''))


def request():
    return dict(request_id='req-muse-to-codex-frequency-synthetic-001',
                **{'from': 'muse', 'to': 'codex'}, skill_id=SKILL,
                source_repo=SOURCE, source_ref=REF,
                source_path=PRODUCT+'msg-from-muse-synthetic.md', kind='handoff')


def receipt(eid, action='acknowledged', claim='11111111-2222-4333-8444-555555555555', evidence=None):
    return dict(event_id=eid, role='codex', action=action, at='2026-11-16T18:00:00+08:00',
                evidence_ref=evidence, claim_id=claim)


class FakeBackend:
    """Commit snapshots, create-only writes and compare-and-swap; only the two allowed repos exist."""
    def __init__(self):
        self.heads = {SOURCE: CURRENT, RELAY: RELAY_REF}
        self.snapshots = {(SOURCE, CURRENT): {}, (SOURCE, REF): {}, (RELAY, RELAY_REF): {}}
        self.reads = []
        self.writes = []
        self.fail_create_before = set()
        self.fail_create_after = set()
        self.counter = 16

    def check(self, repo):
        if repo not in (SOURCE, RELAY):
            raise AssertionError('Cross-line repository access attempted: ' + repo)

    def resolve(self, repo, ref):
        self.check(repo)
        return self.heads[repo] if ref in ('main', None) else ref

    def put(self, repo, ref, path, value):
        self.check(repo)
        self.snapshots.setdefault((repo, ref), {})[path] = value

    def current_put(self, repo, path, value):
        old = self.heads[repo]
        self.counter += 1
        new = format(self.counter, '040x')
        self.snapshots[repo, new] = dict(self.snapshots[repo, old])
        self.snapshots[repo, new][path] = value
        self.heads[repo] = new

    def current(self, repo, path):
        return self.snapshots[repo, self.heads[repo]][path]

    def head(self, repo):
        self.check(repo)
        return self.heads[repo]

    def list(self, repo, prefix, ref):
        ref = self.resolve(repo, ref)
        self.reads.append((repo, prefix, ref))
        if (repo, ref) not in self.snapshots:
            raise F.NotFound('Synthetic missing tree')
        return [dict(path=p, sha=blob(b)) for p, b in sorted(self.snapshots[repo, ref].items())
                if p.startswith(prefix)]

    def read(self, repo, path, ref='main'):
        ref = self.resolve(repo, ref)
        self.reads.append((repo, path, ref))
        try:
            value = self.snapshots[repo, ref][path]
        except KeyError:
            raise F.NotFound('Synthetic missing file')
        return value, blob(value)

    def create(self, repo, path, value):
        self.check(repo)
        if path in self.fail_create_before:
            self.fail_create_before.remove(path)
            raise RuntimeError('Synthetic create unavailable before write')
        if path in self.snapshots[repo, self.heads[repo]]:
            raise F.Conflict('Synthetic create-only conflict')
        self.current_put(repo, path, value)
        self.writes.append(dict(method='create', repo=repo, path=path, value=bytes(value)))
        if path in self.fail_create_after:
            self.fail_create_after.remove(path)
            raise RuntimeError('Synthetic response lost after create')
        return blob(value)

    def update(self, repo, path, value, oldsha):
        self.check(repo)
        try:
            old = self.current(repo, path)
        except KeyError:
            raise F.NotFound('Synthetic CAS missing path')
        if blob(old) != oldsha:
            raise F.Conflict('Synthetic CAS conflict')
        self.current_put(repo, path, value)
        self.writes.append(dict(method='update', repo=repo, path=path, value=bytes(value)))
        return blob(value)


class ConsumerIsolation(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='frequency-consumer-isolated-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.cfg = config()
        self.backend = FakeBackend()
        self.req = request()
        self.sent = []
        self.guards = [patch.object(socket, 'create_connection', side_effect=AssertionError('Network forbidden')),
                       patch.object(subprocess, 'run', side_effect=AssertionError('Process/API forbidden')),
                       patch.object(subprocess, 'Popen', side_effect=AssertionError('Worker/queue forbidden'))]
        for guard in self.guards:
            guard.start()
            self.addCleanup(guard.stop)
        self.install_source()
        self.publish_request()
        self.c = F.Consumer(self.cfg, self.base, self.backend, self.deliver)

    def deliver(self, req, eid, claim):
        self.sent.append(dict(request=copy.deepcopy(req), event_id=eid, claim=copy.deepcopy(claim)))
        return 'Queued message synthetic to ' + THREAD

    def install_source(self, req=None, message=None):
        req = req or self.req
        self.backend.put(SOURCE, req['source_ref'], req['source_path'],
                         (message if message is not None else PREFIX+'\n\n# 合成派单\n仅供本地隔离测试。\n').encode())
        self.backend.put(SOURCE, req['source_ref'], SOP,
                         b'---\nname: sop-frequency-skill-auto\n---\n# Synthetic frequency SOP\n')
        self.set_status(dict(skill_id=SKILL, sop='sop-frequency-skill-auto', stage='brief_ready',
                             owner=None, claimed_at=None, blocked=None,
                             rounds=dict(spec=0, dev=0, test=0)))

    def set_status(self, status):
        self.backend.current_put(SOURCE, PRODUCT+'status.json', raw(status))

    def publish_request(self, req=None, filename=None):
        req = req or self.req
        self.backend.current_put(RELAY, 'requests/'+(filename or req['request_id']+'.json'), raw(req))

    def poll(self):
        # A denied request may be represented as a health result or a validation exception.
        # Side-effect assertions remain the safety condition in either representation.
        try:
            return self.c.poll_once()
        except (ValueError, F.NotFound, F.Conflict, RuntimeError):
            return None

    @property
    def eid(self):
        return F.event_id(self.req)

    @property
    def event_receipt(self):
        return 'receipts/codex/'+self.eid+'.json'

    @property
    def request_receipt(self):
        return 'receipts/codex/'+self.req['request_id']+'.json'

    def remote_receipt(self, path):
        return json.loads(self.backend.current(RELAY, path))

    def assert_no_effects(self):
        self.assertEqual(self.backend.writes, [])
        self.assertEqual(self.sent, [])
        self.assertTrue(all(repo in (SOURCE, RELAY) for repo, _, _ in self.backend.reads))

    def diagnostics(self):
        return [w for w in self.backend.writes if w['path'].endswith('.md')]

    def assert_no_claim_or_delivery(self, diagnostic_count=None):
        self.assertEqual(self.sent, [])
        self.assertFalse([w for w in self.backend.writes if w['path'].endswith('.json')],
                         'Wrong or missing input must never create or update a receipt claim')
        self.assertTrue(all(repo in (SOURCE, RELAY) for repo, _, _ in self.backend.reads))
        self.assertTrue(self.backend.writes, 'A specific rejection/missing-input diagnostic is required')
        for row in self.backend.writes:
            self.assertEqual(row['repo'], RELAY)
            self.assertRegex(row['path'], r'^receipts/codex/issue-[a-f0-9]{64}\.md$')
            self.assertEqual(row['method'], 'create')
            text=row['value'].decode('utf-8')
            self.assertIn('reason_code', text)
        if diagnostic_count is not None:
            self.assertEqual(len(self.diagnostics()), diagnostic_count)

    def audit_records(self):
        return [json.loads(p.read_text(encoding='utf-8-sig')) for p in (self.base/'audit').glob('*.json')]

    def assert_reason_code(self, code):
        records=self.audit_records()
        self.assertTrue(records, 'A local reason-coded record is required')
        self.assertIn(code, [r.get('reason_code') for r in records])
        self.assertTrue(any(code in row['value'].decode('utf-8') for row in self.diagnostics()))

    def activate_fixed_message(self, index):
        fixture=FIXED_MESSAGES[index]
        body=base64.b64decode(fixture['message_base64'])
        self.assertEqual(hashlib.sha256(body).hexdigest(),fixture['message_sha256'])
        current=self.backend.snapshots[RELAY,self.backend.heads[RELAY]]
        del current['requests/'+self.req['request_id']+'.json']
        self.req=copy.deepcopy(fixture['request'])
        self.install_source(self.req,message=body.decode('utf-8'))
        self.publish_request()
        first=[line.strip().replace('`','')for line in body.decode('utf-8').splitlines()if line.strip()][:5]
        self.assertFalse(any(line.lstrip('# ').startswith(PREFIX)for line in first),
                         'Regression fixture must not accidentally satisfy the old exact-prefix gate')
        actual,_=self.backend.read(SOURCE,self.req['source_path'],self.req['source_ref'])
        self.assertEqual(actual,body)

    def assert_only_receipts_written(self):
        self.assertTrue(self.backend.writes)
        for row in self.backend.writes:
            self.assertEqual(row['repo'], RELAY)
            self.assertTrue(row['path'].startswith('receipts/codex/'), row['path'])

    def evidence(self, path=None):
        path = path or PRODUCT+'01-dev-spec.md'
        self.backend.put(SOURCE, EVIDENCE_REF, path, b'# Synthetic committed evidence\n')
        return EVIDENCE_REF+':'+path

    def local_claim(self):
        return json.loads((self.base/'state'/ (self.eid+'.json')).read_text(encoding='utf-8-sig'))

    def test_binding_accepts_only_the_fixed_frequency_identity(self):
        F.validate_binding(self.cfg)
        mutations = [('pipeline_repository', 'Haihe2026/myplanestroe'),
                     ('relay_repository', 'Haihe2026/myplanestroe'), ('sop_path', 'docs/sop/sop-image-gen-skill-auto.md'),
                     ('product_root', 'pipeline/'), ('consumer_id', 'codex-image-pc'),
                     ('requests_root', 'pipeline/relay/requests/'), ('receipts_root', 'receipts/'),
                     ('role', 'muse'), ('pipeline_branch', 'other'), ('relay_branch', 'other')]
        for key, value in mutations:
            with self.subTest(key=key):
                cfg = copy.deepcopy(self.cfg);cfg[key] = value
                with self.assertRaises(ValueError): F.validate_binding(cfg)
        self.assert_no_effects()

    def test_chat_binding_never_allows_a_worker_or_another_chat(self):
        for change in ('thread', 'mode', 'worker', 'implicit', 'sender'):
            cfg = copy.deepcopy(self.cfg)
            if change == 'thread': cfg['delivery']['thread_id'] = '11111111-2222-4333-8444-555555555555'
            if change == 'mode': cfg['delivery']['mode'] = 'worker'
            if change == 'worker': cfg['worker_enabled'] = True
            if change == 'implicit': cfg['explicit_requests_only'] = False
            if change == 'sender': cfg['allowed_senders'] = ['muse', 'claude']
            with self.subTest(change=change), self.assertRaises(ValueError): F.validate_binding(cfg)
        self.assert_no_effects()

    def test_exact_eight_request_fields_and_original_digest(self):
        F.validate_request(self.req, self.cfg)
        expected='evt-'+hashlib.sha256(json.dumps(self.req,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        self.assertEqual(self.eid, expected)
        for key in ('extra', 'schema_version', 'consumer_id'):
            req=copy.deepcopy(self.req);req[key]='synthetic'
            with self.subTest(extra=key), self.assertRaises(ValueError): F.validate_request(req,self.cfg)
        self.assert_no_effects()

    def test_extra_request_fields_are_diagnosed_without_creating_a_claim(self):
        self.req['execute']='synthetic-disallowed-extra'
        self.publish_request();self.poll()
        self.assert_no_claim_or_delivery(1)
        self.assert_reason_code('REQUEST_FIELDS_INVALID')

    def test_wrong_repository_is_diagnosed_without_claim_or_foreign_access(self):
        self.req['source_repo']='Haihe2026/myplanestroe';self.publish_request()
        self.poll();self.assert_no_claim_or_delivery(1)
        self.assert_reason_code('SOURCE_REPO_NOT_ALLOWED')
        count=len(self.backend.writes);self.poll();self.poll()
        self.assertEqual(len(self.backend.writes),count,'Repeated polls must not republish the same rejection')

    def test_noncanonical_traversal_encoded_and_other_product_paths_never_claim(self):
        bad=[PRODUCT+'../other/msg.md', '/'+PRODUCT+'msg.md', PRODUCT+'%2e%2e/msg.md',
             PRODUCT.replace('/','\\')+'msg.md', PRODUCT+'./msg.md',
             'pipeline/products/other-skill/msg.md', PRODUCT+'double//msg.md']
        for path in bad:
            self.req['source_path']=path;self.publish_request();self.poll()
            self.assert_no_claim_or_delivery()
        self.assert_reason_code('SOURCE_PATH_NOT_ALLOWED')

    def test_nonfixed_source_ref_is_diagnosed_without_claim(self):
        self.req['source_ref']='main';self.publish_request();self.poll();self.assert_no_claim_or_delivery(1)
        self.assert_reason_code('SOURCE_REF_NOT_FIXED')

    def test_source_ref_that_exists_only_in_transport_repo_is_not_a_business_source(self):
        self.req['source_ref']=RELAY_REF;self.publish_request()
        self.backend.put(RELAY,RELAY_REF,self.req['source_path'],PREFIX.encode())
        self.poll();self.assert_no_claim_or_delivery(1)
        self.assert_reason_code('SOURCE_MESSAGE_NOT_FOUND')

    def test_missing_fixed_source_is_reported_pending_without_claim(self):
        self.req['source_path']=PRODUCT+'missing-message.md';self.publish_request()
        self.poll();self.assert_no_claim_or_delivery(1)
        self.assert_reason_code('SOURCE_MESSAGE_NOT_FOUND')

    def test_nonmatching_message_title_does_not_override_valid_machine_source(self):
        self.backend.put(SOURCE,REF,self.req['source_path'],
                         b'Image pipeline Haihe2026/myplanestroe\n# Same-named skill\n')
        self.poll();self.assertEqual(len(self.sent),1);self.assert_only_receipts_written()

    def test_prefix_after_five_nonempty_lines_is_not_a_hard_gate(self):
        self.backend.put(SOURCE,REF,self.req['source_path'],
                         ('\n'.join(['# Wrong line']*6+[PREFIX])).encode())
        self.poll();self.assertEqual(len(self.sent),1);self.assert_only_receipts_written()

    def test_real_spec_message_with_list_identity_fields_is_consumed(self):
        self.activate_fixed_message(0)
        self.poll();self.assertEqual(len(self.sent),1)
        self.assertEqual(self.sent[0]['request'],self.req)
        self.assertEqual(self.remote_receipt(self.event_receipt)['action'],'claimed')
        self.assertEqual(self.remote_receipt(self.request_receipt)['claim_id'],self.local_claim()['claim_id'])
        self.assertFalse(self.diagnostics(),'A real equivalent-format message is not a rejected task')

    def test_real_status_check_message_without_sop_field_is_consumed(self):
        self.activate_fixed_message(1)
        self.poll();self.assertEqual(len(self.sent),1)
        self.assertEqual(self.sent[0]['request'],self.req)
        self.assertEqual(self.remote_receipt(self.event_receipt)['action'],'claimed')
        self.assertEqual(self.remote_receipt(self.request_receipt)['claim_id'],self.local_claim()['claim_id'])
        self.assertFalse(self.diagnostics(),'Missing prose identity is not an authorization rejection')

    def test_missing_text_identity_produces_a_safe_bound_chat_notice(self):
        self.install_source(message='# 合成固定任务\n请按已登记仓库资料在当前聊天处理。\n')
        self.poll();self.assertEqual(len(self.sent),1)
        claim=self.remote_receipt(self.event_receipt)
        result=subprocess.CompletedProcess([],0,stdout=b'Queued message synthetic',stderr=b'')
        with patch.object(F,'codex_executable',return_value=Path(sys.executable)), \
                patch.object(subprocess,'run',return_value=result) as run:
            self.c._deliver(self.req,self.eid,claim)
        argv=run.call_args.args[0];message=argv[5]
        self.assertEqual(argv[:4],[str(Path(sys.executable)),'queue','--thread',THREAD])
        for value in (SOURCE,RELAY,SOP,REF,self.req['source_path'],claim['claim_id'],'TEXT_IDENTITY_INCOMPLETE'):
            self.assertIn(value,message)
        self.assertFalse(self.diagnostics())

    def test_unknown_source_read_failure_is_not_a_rejection_or_claim(self):
        read=self.backend.read
        def unavailable(repo,path,ref='main'):
            if repo==SOURCE and path==self.req['source_path']:
                raise RuntimeError('Synthetic unknown source read outcome')
            return read(repo,path,ref)
        with patch.object(self.backend,'read',side_effect=unavailable):
            self.poll();self.poll()
        self.assert_no_effects()
        self.assertFalse(any(r.get('state')=='rejected'for r in self.audit_records()))

    def test_unknown_diagnostic_write_response_is_reconciled_without_duplicate_notice(self):
        self.req['source_repo']='Haihe2026/myplanestroe';self.publish_request()
        create=self.backend.create;lost=[False]
        def accepted_then_lost(repo,path,value):
            result=create(repo,path,value)
            if path.endswith('.md')and not lost[0]:
                lost[0]=True
                raise RuntimeError('Synthetic diagnostic response lost after create')
            return result
        with patch.object(self.backend,'create',side_effect=accepted_then_lost):
            self.poll()
        self.poll();self.poll()
        self.assert_no_claim_or_delivery(1)

    def test_missing_fixed_sop_is_reported_pending_without_claim(self):
        del self.backend.snapshots[SOURCE,REF][SOP]
        self.poll();self.assert_no_claim_or_delivery(1)
        self.assert_reason_code('SOP_NOT_FOUND')

    def test_missing_current_status_can_be_filled_then_consumed_without_new_claims(self):
        del self.backend.snapshots[SOURCE,self.backend.heads[SOURCE]][PRODUCT+'status.json']
        self.poll();self.assert_no_claim_or_delivery(1)
        self.assert_reason_code('STATUS_NOT_FOUND')
        count=len(self.backend.writes);self.poll();self.poll()
        self.assertEqual(len(self.backend.writes),count,'Missing status is not a new diagnostic every minute')
        self.set_status(dict(skill_id=SKILL,sop='sop-frequency-skill-auto',stage='brief_ready',owner=None,blocked=None))
        self.poll();self.assertEqual(len(self.sent),1);self.assert_only_receipts_written()

    def test_current_status_wrong_skill_sop_or_stage_is_rejected(self):
        for field, value in [('skill_id','same-name-other-skill'),('sop','sop-image-gen-skill-auto'),('stage','started')]:
            status=dict(skill_id=SKILL,sop='sop-frequency-skill-auto',stage='brief_ready',owner=None,blocked=None)
            status[field]=value;self.set_status(status);self.poll();self.assert_no_claim_or_delivery()
        self.assert_reason_code('STATUS_IDENTITY_INVALID')

    def test_request_filename_must_match_the_request_id(self):
        snap=self.backend.snapshots[RELAY,self.backend.heads[RELAY]]
        snap['requests/req-other-name.json']=snap.pop('requests/'+self.req['request_id']+'.json')
        self.poll();self.assert_no_claim_or_delivery(1)
        self.assert_reason_code('REQUEST_ID_FILENAME_MISMATCH')

    def test_positive_request_claims_both_keys_and_delivers_only_to_injected_chat(self):
        self.poll();self.assertEqual(len(self.sent),1)
        a=self.remote_receipt(self.event_receipt);b=self.remote_receipt(self.request_receipt)
        self.assertEqual(a,b);self.assertEqual(a['action'],'claimed');self.assertEqual(a['event_id'],self.eid)
        self.assertEqual(set(a),{'event_id','role','action','at','evidence_ref','claim_id'})
        self.assertEqual(self.sent[0]['event_id'],self.eid);self.assertEqual(self.sent[0]['request'],self.req)
        self.assertEqual(self.sent[0]['claim']['claim_id'],a['claim_id'])
        self.assertEqual(self.local_claim()['claim_id'],a['claim_id'])
        self.assertEqual(self.local_claim()['request'],self.req);self.assert_only_receipts_written()

    def test_fixed_source_prefix_can_use_markdown_code_ticks_for_repo_and_sop(self):
        text='# 频次流水线（仓库 `'+SOURCE+'`，SOP `'+SOP+'`）\n\n# 合成固定任务\n'
        self.backend.put(SOURCE,REF,self.req['source_path'],text.encode())
        self.poll();self.assertEqual(len(self.sent),1);self.assert_only_receipts_written()

    def test_actual_delivery_builds_only_the_bound_chat_queue_command(self):
        claim=receipt(self.eid,action='claimed')
        result=subprocess.CompletedProcess([],0,stdout=b'Queued message synthetic',stderr=b'')
        with patch.object(F,'codex_executable',return_value=Path(sys.executable)), \
                patch.object(subprocess,'run',return_value=result) as run:
            confirmation=self.c._deliver(self.req,self.eid,claim)
        self.assertEqual(confirmation,'Queued message');run.assert_called_once()
        argv=run.call_args.args[0]
        self.assertEqual(argv[:4],[str(Path(sys.executable)),'queue','--thread',THREAD])
        self.assertEqual(argv[4],'--message');self.assertEqual(argv[-1],'--resume');self.assertEqual(len(argv),7)
        self.assertNotIn('exec',argv);self.assertNotIn('worker',argv)
        for value in [SOURCE,RELAY,REF,self.req['source_path'],claim['claim_id']]:self.assertIn(value,argv[5])
        self.assertEqual(self.backend.writes,[]);self.assertEqual(self.sent,[])

    def test_invalid_startup_binding_exits_nonzero_before_any_network_or_child_command(self):
        # This controlled child only runs the consumer's invalid-config error path.
        # Its bootstrap forbids sockets and every further subprocess before loading core.
        folder=self.base/'invalid-startup';folder.mkdir()
        script=folder/'frequency_consumer.py';script.write_bytes(Path(F.__file__).read_bytes())
        cfg=copy.deepcopy(self.cfg);cfg['sop_path']='docs/sop/sop-image-gen-skill-auto.md'
        (folder/'config.local.json').write_bytes(raw(cfg))
        bootstrap='''import os,runpy,socket,subprocess,sys
def deny(*a,**k):raise AssertionError("network or nested command forbidden in CLI test")
for name in ("connect","connect_ex","sendto"):setattr(socket.socket,name,deny)
for name in ("create_connection","getaddrinfo","gethostbyname"):setattr(socket,name,deny)
subprocess.run=deny;subprocess.Popen=deny;os.system=deny
sys.argv=[sys.argv[1],"--once"]
runpy.run_path(sys.argv[0],run_name="__main__")
'''
        child=REAL_POPEN([sys.executable,'-X','utf8','-I','-c',bootstrap,str(script)],
                         stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                         creationflags=0x08000000 if os.name=='nt' else 0)
        stdout,stderr=child.communicate(timeout=15)
        self.assertNotEqual(child.returncode,0,(stdout,stderr))
        failure=json.loads(stdout.decode('utf-8'))
        self.assertEqual(failure['state'],'error')
        self.assertIsInstance(failure.get('error_type'),str)
        self.assertFalse((folder/'consumer').exists())
        self.assert_no_effects()

    def test_empty_request_directory_never_calls_queue_or_an_api_process(self):
        del self.backend.snapshots[RELAY,self.backend.heads[RELAY]]['requests/'+self.req['request_id']+'.json']
        self.poll();self.assert_no_effects()

    def test_request_for_muse_is_not_consumed_by_codex(self):
        self.req['to']='muse';self.req['from']='codex';self.publish_request()
        self.poll();self.assert_no_effects()

    def test_duplicate_poll_does_not_reclaim_or_redeliver(self):
        self.poll();count=len(self.backend.writes);claim=self.remote_receipt(self.event_receipt)['claim_id']
        self.poll();self.assertEqual(len(self.sent),1);self.assertEqual(len(self.backend.writes),count)
        self.assertEqual(self.remote_receipt(self.event_receipt)['claim_id'],claim)

    def test_restart_retains_delivery_intent_and_original_claim(self):
        self.poll();claim=self.remote_receipt(self.event_receipt)['claim_id'];count=len(self.backend.writes)
        self.c=F.Consumer(self.cfg,self.base,self.backend,self.deliver);self.poll()
        self.assertEqual(len(self.sent),1);self.assertEqual(len(self.backend.writes),count)
        self.assertEqual(self.local_claim()['claim_id'],claim)

    def test_event_key_acknowledged_is_silent_even_if_alias_is_missing(self):
        self.backend.current_put(RELAY,self.event_receipt,raw(receipt(self.eid)))
        self.poll();self.assert_no_effects()

    def test_request_key_acknowledged_is_silent_even_if_event_key_is_missing(self):
        self.backend.current_put(RELAY,self.request_receipt,raw(receipt(self.eid)))
        self.poll();self.assert_no_effects()

    def test_legacy_flat_receipt_keys_are_checked_before_claim(self):
        for suffix in [self.eid,self.req['request_id']]:
            path='receipts/'+suffix+'.json';self.backend.current_put(RELAY,path,raw(receipt(self.eid)))
            self.poll();self.assert_no_effects()

    def test_remote_claim_without_this_local_claim_is_not_taken_over(self):
        self.backend.current_put(RELAY,self.event_receipt,raw(receipt(self.eid,action='claimed')))
        self.poll();self.assert_no_effects()

    def test_request_alias_half_claim_recovers_the_same_uuid_after_restart(self):
        self.backend.fail_create_before.add(self.event_receipt)
        self.poll();self.assertEqual(self.sent,[])
        alias=self.remote_receipt(self.request_receipt);self.assertEqual(alias['action'],'claimed')
        self.c=F.Consumer(self.cfg,self.base,self.backend,self.deliver);self.poll()
        self.assertEqual(len(self.sent),1)
        self.assertEqual(self.remote_receipt(self.event_receipt)['claim_id'],alias['claim_id'])
        self.assertEqual(len([w for w in self.backend.writes if w['path']==self.request_receipt]),1)

    def test_lost_alias_claim_response_is_reconciled_without_second_claim(self):
        self.backend.fail_create_after.add(self.request_receipt)
        self.poll();self.assertEqual(self.sent,[]);original=self.remote_receipt(self.request_receipt)
        self.poll();self.assertEqual(len(self.sent),1)
        self.assertEqual(self.remote_receipt(self.event_receipt)['claim_id'],original['claim_id'])
        self.assertEqual(len([w for w in self.backend.writes if w['path']==self.request_receipt]),1)

    def test_unknown_queue_outcome_is_never_blindly_resent(self):
        def uncertain(req,eid,claim):
            self.deliver(req,eid,claim);raise TimeoutError('Synthetic accepted-or-not unknown')
        self.c=F.Consumer(self.cfg,self.base,self.backend,uncertain)
        self.poll();self.assertEqual(len(self.sent),1)
        self.c=F.Consumer(self.cfg,self.base,self.backend,self.deliver);self.poll();self.poll()
        self.assertEqual(len(self.sent),1);self.assertEqual(self.remote_receipt(self.event_receipt)['action'],'claimed')

    def test_ack_rejects_the_wrong_claim_id_before_any_update(self):
        self.poll();count=len(self.backend.writes)
        with self.assertRaises((ValueError,RuntimeError)):
            self.c.ack(self.eid,'99999999-2222-4333-8444-555555555555',self.evidence())
        self.assertEqual(len(self.backend.writes),count)

    def test_ack_rejects_another_product_evidence_path(self):
        self.poll();count=len(self.backend.writes);claim=self.local_claim()['claim_id']
        wrong=self.evidence('pipeline/products/other-skill/01-dev-spec.md')
        with self.assertRaises((ValueError,RuntimeError)):
            self.c.ack(self.eid,claim,wrong)
        self.assertEqual(len(self.backend.writes),count)

    def test_ack_rejects_uncommitted_or_nonfixed_evidence(self):
        self.poll();count=len(self.backend.writes);claim=self.local_claim()['claim_id']
        for evidence in ['main:'+PRODUCT+'01-dev-spec.md','f'*40+':'+PRODUCT+'01-dev-spec.md']:
            with self.subTest(evidence=evidence),self.assertRaises((ValueError,RuntimeError,F.NotFound)):
                self.c.ack(self.eid,claim,evidence)
        self.assertEqual(len(self.backend.writes),count)

    def test_ack_updates_both_keys_with_original_claim_and_fixed_business_evidence(self):
        self.poll();claim=self.local_claim()['claim_id'];evidence=self.evidence()
        self.c.ack(self.eid,claim,evidence)
        a=self.remote_receipt(self.event_receipt);b=self.remote_receipt(self.request_receipt)
        self.assertEqual(a,b);self.assertEqual(a['claim_id'],claim)
        self.assertEqual(a['action'],'acknowledged');self.assertEqual(a['evidence_ref'],evidence)
        self.assertEqual(self.local_claim()['action'],'acknowledged');self.assert_only_receipts_written()

    def test_same_ack_is_idempotent_without_extra_remote_writes(self):
        self.poll();claim=self.local_claim()['claim_id'];evidence=self.evidence()
        self.c.ack(self.eid,claim,evidence);count=len(self.backend.writes)
        self.c.ack(self.eid,claim,evidence);self.assertEqual(len(self.backend.writes),count)

    def test_remote_acknowledgement_is_reconciled_without_redelivery(self):
        self.poll();claim=self.local_claim()['claim_id'];evidence=self.evidence()
        for path in [self.event_receipt,self.request_receipt]:
            self.backend.current_put(RELAY,path,raw(receipt(self.eid,claim=claim,evidence=evidence)))
        count=len(self.backend.writes);self.poll()
        self.assertEqual(len(self.sent),1);self.assertEqual(len(self.backend.writes),count)
        self.assertEqual(self.local_claim()['action'],'acknowledged')

    def test_same_request_id_changed_body_cannot_create_a_second_event_claim(self):
        self.poll();count=len([w for w in self.backend.writes if w['path'].endswith('.json')]);old=self.eid
        self.req['source_ref']='b'*40;self.install_source();self.publish_request();new=self.eid
        self.assertNotEqual(new,old);self.poll()
        self.assertEqual(len(self.sent),1)
        self.assertEqual(len([w for w in self.backend.writes if w['path'].endswith('.json')]),count)
        self.assertEqual(len(self.diagnostics()),1)
        self.assert_reason_code('RECEIPT_IDENTITY_INVALID')


if __name__ == '__main__':
    unittest.main(verbosity=2)
