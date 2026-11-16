"""Isolated frequency consumer tests: no GitHub, Codex queue, worker or product execution."""
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

    def test_wrong_repository_including_cross_line_same_skill_has_zero_effects(self):
        self.req['source_repo']='Haihe2026/myplanestroe';self.publish_request()
        self.poll();self.assert_no_effects()

    def test_noncanonical_traversal_encoded_and_other_product_paths_have_zero_effects(self):
        bad=[PRODUCT+'../other/msg.md', '/'+PRODUCT+'msg.md', PRODUCT+'%2e%2e/msg.md',
             PRODUCT.replace('/','\\')+'msg.md', PRODUCT+'./msg.md',
             'pipeline/products/other-skill/msg.md', PRODUCT+'double//msg.md']
        for path in bad:
            self.req['source_path']=path;self.publish_request();self.poll()
            self.assert_no_effects()

    def test_nonfixed_source_ref_has_zero_effects(self):
        self.req['source_ref']='main';self.publish_request();self.poll();self.assert_no_effects()

    def test_source_ref_that_exists_only_in_transport_repo_is_not_a_business_source(self):
        self.req['source_ref']=RELAY_REF;self.publish_request()
        self.backend.put(RELAY,RELAY_REF,self.req['source_path'],PREFIX.encode())
        self.poll();self.assert_no_effects()

    def test_missing_fixed_source_has_zero_effects(self):
        self.req['source_path']=PRODUCT+'missing-message.md';self.publish_request()
        self.poll();self.assert_no_effects()

    def test_message_prefix_for_another_line_is_rejected_before_claim(self):
        self.backend.put(SOURCE,REF,self.req['source_path'],
                         b'Image pipeline Haihe2026/myplanestroe\n# Same-named skill\n')
        self.poll();self.assert_no_effects()

    def test_prefix_too_far_down_cannot_hide_the_wrong_message_header(self):
        self.backend.put(SOURCE,REF,self.req['source_path'],
                         ('\n'.join(['# Wrong line']*6+[PREFIX])).encode())
        self.poll();self.assert_no_effects()

    def test_missing_fixed_sop_has_zero_effects(self):
        del self.backend.snapshots[SOURCE,REF][SOP]
        self.poll();self.assert_no_effects()

    def test_missing_current_status_can_be_filled_then_consumed_without_new_claims(self):
        del self.backend.snapshots[SOURCE,self.backend.heads[SOURCE]][PRODUCT+'status.json']
        self.poll();self.assert_no_effects()
        self.set_status(dict(skill_id=SKILL,sop='sop-frequency-skill-auto',stage='brief_ready',owner=None,blocked=None))
        self.poll();self.assertEqual(len(self.sent),1);self.assert_only_receipts_written()

    def test_current_status_wrong_skill_sop_or_stage_is_rejected(self):
        for field, value in [('skill_id','same-name-other-skill'),('sop','sop-image-gen-skill-auto'),('stage','started')]:
            status=dict(skill_id=SKILL,sop='sop-frequency-skill-auto',stage='brief_ready',owner=None,blocked=None)
            status[field]=value;self.set_status(status);self.poll();self.assert_no_effects()

    def test_request_filename_must_match_the_request_id(self):
        snap=self.backend.snapshots[RELAY,self.backend.heads[RELAY]]
        snap['requests/req-other-name.json']=snap.pop('requests/'+self.req['request_id']+'.json')
        self.poll();self.assert_no_effects()

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
        self.assertEqual(json.loads(stdout.decode('utf-8')),
                         {'state':'error','error_type':'ValueError'})
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
        self.poll();count=len(self.backend.writes);old=self.eid
        self.req['source_ref']='b'*40;self.install_source();self.publish_request();new=self.eid
        self.assertNotEqual(new,old);self.poll()
        self.assertEqual(len(self.sent),1);self.assertEqual(len(self.backend.writes),count)


if __name__ == '__main__':
    unittest.main(verbosity=2)
