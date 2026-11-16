"""Dedicated frequency transport supervisor: ordinary polling, no AI heartbeat."""
import contextlib
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parent

def now():return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(timespec='seconds')

def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    os.replace(temp,path)

@contextlib.contextmanager
def singleton():
    path=ROOT/'supervisor/state/poll.lock';path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a+b') as handle:
        if handle.seek(0,2)==0:handle.write(b'0');handle.flush()
        handle.seek(0)
        if os.name=='nt':
            import msvcrt
            try:msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
            except OSError:yield False;return
        else:
            import fcntl
            try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except OSError:yield False;return
        yield True

def main():
    with singleton() as acquired:
        if not acquired:return
        while not(ROOT/'STOP').exists():
            started=time.monotonic()
            health=dict(consumer_id='codex-frequency-pc',pid=os.getpid(),checked_at=now(),state='polling',interval_seconds=60,ai_worker=False)
            write_json(ROOT/'supervisor/state/health.json',health)
            try:
                proc=subprocess.run([sys.executable,str(ROOT/'frequency_consumer.py'),'--once'],cwd=ROOT,
                                    capture_output=True,timeout=240,creationflags=0x08000000 if os.name=='nt'else 0)
                health.update(state='watching'if proc.returncode==0 else'retrying',consumer_exit=proc.returncode)
                # Local private output only; public registration never includes machine paths.
                (ROOT/'supervisor/state/last-poll.stdout.txt').write_bytes(proc.stdout)
                (ROOT/'supervisor/state/last-poll.stderr.txt').write_bytes(proc.stderr)
            except Exception as exc:health.update(state='retrying',error_type=type(exc).__name__)
            health['checked_at']=now();write_json(ROOT/'supervisor/state/health.json',health)
            while time.monotonic()-started<60:
                if(ROOT/'STOP').exists():return
                time.sleep(1)
        write_json(ROOT/'supervisor/state/health.json',dict(consumer_id='codex-frequency-pc',pid=os.getpid(),checked_at=now(),state='stopped'))

if __name__=='__main__':main()
