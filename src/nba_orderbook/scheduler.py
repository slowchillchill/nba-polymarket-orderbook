"""半小时发现与一次性定时提交；状态仅属于产品运行。"""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from contextlib import contextmanager
from .model import Game, dt, iso, now, group_games, peak_concurrency
from .collector import write_json
from .kaggle import SubmissionNotAttempted, SubmissionRejected

ACTIVE={'QUEUED','RUNNING','CANCEL_REQUESTED'}
TERMINAL={'COMPLETE','ERROR','CANCEL_ACKNOWLEDGED','CANCELLED'}

class Store:
    def __init__(self,path):
        self.path=Path(path)
        self.path.mkdir(parents=True,exist_ok=True)
    @contextmanager
    def locked(self):
        with (self.path/'scheduler.lock').open('a') as f:
            fcntl.flock(f,fcntl.LOCK_EX)
            p=self.path/'state.json'
            state=json.loads(p.read_text()) if p.exists() else {'games':{},'batches':{},'anomalies':[]}
            yield state
            self.save(state)
    def save(self,state):
        p=self.path/'state.json.tmp'
        with p.open('w') as stream:
            json.dump(state,stream,ensure_ascii=False,default=str)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(p,self.path/'state.json')
        parent=os.open(self.path,os.O_RDONLY|os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)

class Timers:
    def schedule(self,batch_id,when,state_dir,config):
        unit='nba-orderbook-'+batch_id
        subprocess.run(['systemctl','stop',unit+'.timer'],check=False,capture_output=True)
        subprocess.run(['systemd-run','--unit='+unit,'--on-calendar='+dt(when).strftime('%Y-%m-%d %H:%M:%S UTC'),
                        '--timer-property=Persistent=true','--property=EnvironmentFile=/etc/nba-orderbook/credentials.env','--property=WorkingDirectory=/opt/nba-orderbook/current',sys.executable,'-m','nba_orderbook.cli','--config',str(config),
                        'scheduler','--submit',batch_id],check=True,capture_output=True)
    def cancel(self,batch_id):
        subprocess.run(['systemctl','stop','nba-orderbook-'+batch_id+'.timer'],check=False,capture_output=True)

def update_registered_sessions(backend,state):
    ended={(b['ref'],b['version']) for b in state['batches'].values()
           if b['state']=='ended' and b.get('ref') and b.get('version')}
    registered={(entry['ref'],entry['version']):entry for entry in backend.known_sessions
                if (entry['ref'],entry['version']) not in ended}
    for batch in state['batches'].values():
        if batch['state']!='ended' and batch.get('ref') and batch.get('version'):
            registered[(batch['ref'],batch['version'])]={'ref':batch['ref'],'version':batch['version']}
    backend.known_sessions=list(registered.values())

async def scan(config,backend,api,timers=None):
    timers=timers or Timers()
    store=Store(config['state_dir'])
    games,anomalies=await api.discover()
    with store.locked() as state:
        retry_batches={bid:b for bid,b in state['batches'].items() if b['state']=='planned' and b.get('error') in (SubmissionNotAttempted.__name__,SubmissionRejected.__name__)}
        state['anomalies']=anomalies
        ambiguous={str(a['game_id']) for a in anomalies if a.get('reason')=='multiple_moneylines'}
        unique={game.game_id for game in games}
        for gid,row in state['games'].items():
            if gid in ambiguous:
                row['ambiguous_market']=True
            elif gid in unique:
                row['ambiguous_market']=False
        update_registered_sessions(backend,state)
        for game in games:
            old=state['games'].get(game.game_id)
            if old:
                previous=Game.from_dict(old['game'])
                previous.reschedule(game.start,game.status,game.started)
                game.start=previous.start
                game.started=previous.started
                game.time_changes=previous.time_changes
                game.postponed_with_time=previous.postponed_with_time
                old['game']=game.to_dict()
                old['waiting_schedule']=game.status.lower()=='postponed' and not game.postponed_with_time
                if old.get('batch_id') in retry_batches:
                    continue
            if old and old.get('batch_id') and state['batches'][old['batch_id']]['state'] not in ('planned','ended','missed_window'):
                continue
            if old and old.get('batch_id') and state['batches'][old['batch_id']]['state']=='ended':
                published=backend.inspect_dataset(f"{config['owner']}/{game.slug}")
                if published and published.get('status','').lower()=='ready' and published.get('summary'):
                    continue
            postponed=game.status.lower()=='postponed' and not game.postponed_with_time
            state['games'][game.game_id]={'game':game.to_dict(),'batch_id':None,'waiting_schedule':postponed,'ambiguous_market':game.game_id in ambiguous}
        for bid,b in list(state['batches'].items()):
            if b['state']=='planned':
                timers.cancel(bid)
                if bid in retry_batches:
                    continue
                del state['batches'][bid]
                for row in state['games'].values():
                    if row.get('batch_id')==bid:
                        row['batch_id']=None
        retry_groups=[]
        ready_retries={}
        for bid,b in retry_batches.items():
            members=[g['game_id'] for g in b['games']]
            eligible=[Game.from_dict(state['games'][gid]['game']) for gid in members
                      if gid in unique and not state['games'][gid].get('waiting_schedule')
                      and not state['games'][gid].get('ambiguous_market')]
            if not eligible:
                continue
            # 尚无已接受的远端实例，按本轮时间重新分组。原身份延续给首个合法组。
            first_group=group_games(eligible)[0]
            retained={g.game_id for g in first_group}
            for gid in members:
                if gid not in retained:
                    state['games'][gid]['batch_id']=None
            b['games']=[g.to_dict() for g in first_group]
            b['submit_at']=iso(min(g.window for g in first_group))
            retry_groups.append(first_group)
            ready_retries[bid]=b
        pending=[Game.from_dict(r['game']) for gid,r in state['games'].items() if gid in unique and not r.get('batch_id') and not r.get('waiting_schedule') and not r.get('ambiguous_market')]
        groups=group_games(pending)
        fixed_groups=[[Game.from_dict(g) for g in b['games']] for b in state['batches'].values()
                      if b['state'] in ('submitted','submission_unknown','submitting')]
        state['required_peak_notebooks']=peak_concurrency(groups+retry_groups+fixed_groups)
        state['observed_active_notebooks']=backend.list_active()
        state['cpu_notebook_capacity']=config.get('cpu_notebook_capacity')
        state['occupancy_coverage']='latest_and_registered_versions_not_exhaustive'
        for group in groups:
            bid=uuid.uuid4().hex[:16]
            b={'id':bid,'games':[g.to_dict() for g in group],'owner':config['owner'],'state':'planned',
               'submit_at':iso(min(g.window for g in group))}
            state['batches'][bid]=b
            for game in group:
                state['games'][game.game_id]['batch_id']=bid
            store.save(state)
            if dt(b['submit_at'])<=now():
                submit_locked(b,store,state,config,backend)
            else:
                timers.schedule(bid,b['submit_at'],store.path,config['_path'])
        for bid,b in ready_retries.items():
            store.save(state)
            if dt(b['submit_at'])<=now():
                submit_locked(b,store,state,config,backend)
            else:
                timers.schedule(bid,b['submit_at'],store.path,config['_path'])
        for b in state['batches'].values():
            if b['state'] in ('submitted','submission_unknown','submitting'):
                if not b.get('ref'):
                    b['ref']=f"{config['owner']}/nba-batch-{b['id']}"
                try:
                    snapshot=backend.snapshot(b['ref'],b.get('version'))
                    b['remote_status']=snapshot['status']
                    if snapshot.get('version'):
                        b['version']=snapshot['version']
                    if snapshot['status'] in TERMINAL:
                        b['state']='ended'
                    elif snapshot['status'] in ACTIVE:
                        b['state']='submitted'
                except Exception as e:
                    b['status_error']=type(e).__name__
        update_registered_sessions(backend,state)
        rows=[]
        for row in state['games'].values():
            game=row['game']; ref=f"{config['owner']}/{game['slug']}"
            published=backend.inspect_dataset(ref)
            if published and published.get('status','').lower()=='ready' and published.get('summary'):
                metadata=backend.read_json(ref,'metadata.json',published['version'])
                rows.append({'slug':game['slug'],'game_start':metadata['start'],'dataset':ref,
                             'final_version':published['version'],'stop_reason':published.get('summary',{}).get('stop_reason'),
                             'quality':published.get('summary',{}).get('quality')})
        if rows:
            backend.catalog(rows,store.path/'catalog')
        state['last_scan']=iso(now())
    return state

def submit_locked(batch,store,state,config,backend):
    if batch['state']!='planned':
        return
    at=now()
    remaining=[g for g in batch['games'] if Game.from_dict(g).deadline>at and not state.get('games',{}).get(g['game_id'],{}).get('ambiguous_market')]
    if not remaining:
        batch['state']='missed_window'
        return
    if at<dt(batch['submit_at']):
        return
    active=backend.list_active()
    capacity=config.get('cpu_notebook_capacity')
    if capacity is not None and len(active)>=capacity:
        batch['capacity_wait_at']=iso(at)
        return
    batch['games']=remaining
    batch['state']='submitting'
    batch['ref']=f"{config['owner']}/nba-batch-{batch['id']}"
    store.save(state)
    try:
        result=backend.submit(batch,Path(config.get('source_dir',Path.cwd())))
        batch.update(result)
        batch['state']='submitted'
    except (SubmissionNotAttempted,SubmissionRejected) as e:
        batch['state']='planned'
        batch['error']=type(e).__name__
    except Exception as e:
        batch['state']='submission_unknown'
        batch['error']=type(e).__name__
    store.save(state)

def submit(config,backend,bid):
    store=Store(config['state_dir'])
    with store.locked() as state:
        submit_locked(state['batches'][bid],store,state,config,backend)
    return state['batches'][bid]
