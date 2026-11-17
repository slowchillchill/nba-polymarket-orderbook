from datetime import timedelta
import json
import gzip
import asyncio
from nba_orderbook.model import Game, dt, group_games
from nba_orderbook.book import Book
from nba_orderbook.polymarket import select_games,Polymarket
from nba_orderbook.collector import Collection

def game(start='2026-11-18T00:30:00Z',gid='1'):
    return Game(gid,'nba-a-b', '10','20','condition',['a','b'],['A','B'],start)

def test_two_hours_is_start_span_not_duration():
    a=game(); b=game('2026-11-18T02:30:00Z','2'); c=game('2026-11-18T02:30:01Z','3')
    assert [[g.game_id for g in x] for x in group_games([c,b,a])]==[['1','2'],['3']]
    assert b.deadline==dt('2026-11-18T08:30:00Z')

def test_time_frozen_only_after_observed_start():
    g=game();g.reschedule('2026-11-18T01:30:00Z',live=True)
    g.reschedule('2026-11-19T01:30:00Z')
    assert g.start=='2026-11-18T01:30:00+00:00'
    assert len(g.time_changes)==1

def test_book_changes_are_absolute_and_snapshot_replaces():
    b=Book(['a','b'])
    b.apply({'event_type':'book','asset_id':'a','bids':[{'price':'.1','size':'4'}]})
    b.apply({'event_type':'price_change','price_changes':[{'asset_id':'a','side':'BUY','price':'.1','size':'2'}]})
    assert b.assets['a']['bids']=={'0.1':'2'}
    b.apply({'event_type':'price_change','price_changes':[{'asset_id':'a','side':'BUY','price':'.1','size':'0'}]})
    assert not b.assets['a']['bids']
    b.apply({'event_type':'book','asset_id':'a','bids':[{'price':'.2','size':'9'}]})
    assert b.assets['a']['bids']=={'0.2':'9'}

def event():
    return {'id':'1','slug':'date-does-not-matter','gameId':5,'startTime':'2026-11-18T00:30:00Z','markets':[
        {'id':'2','conditionId':'c','sportsMarketType':'moneyline','version':'v2','clobTokenIds':'["wrong","wrong2"]',
         'positionIds':['a','b'],'outcomes':'["A","B"]'}]}

def test_unique_market_and_version():
    e=event();games,errors=select_games([e,e])
    assert not errors and len(games)==1 and games[0].assets==['a','b']
    e['markets'].append({**e['markets'][0],'conditionId':'d'})
    games,errors=select_games([e]);assert games==[] and errors[0]['reason']=='multiple_moneylines'

async def test_closed_is_not_resolution():
    class API(Polymarket):
        async def get(self,*a):
            return {'data':[{'condition_id':'condition','closed':True,'status':'proposed'}]}
    assert await API().resolution(game()) is None

async def test_deadline_is_not_resolution(tmp_path):
    from nba_orderbook.model import now,iso
    g=game(iso(now()-timedelta(hours=7)))
    class API:
        async def resolution(self,g):return None
    result=await Collection(g,tmp_path,API(),now()+timedelta(hours=1)).run()
    assert result['stop_reason']=='six_hour_deadline' and result['resolution'] is None
    with gzip.open(tmp_path/'orderbook.jsonl.gz','rt') as f: assert f.read()==''

async def test_raw_frames_and_both_reconnect_snapshots(tmp_path):
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(minutes=5)))
    class API:
        async def resolution(self,g): return None
        async def refresh(self,g): return g
    frames=['not json',json.dumps({'event_type':'unknown','value':7}),json.dumps([
        {'event_type':'book','asset_id':'a','bids':[{'price':'.2','size':'3'}]},
        {'event_type':'book','asset_id':'b','asks':[{'price':'.8','size':'4'}]}])]
    class Socket:
        def __aiter__(self):return self
        async def __anext__(self):
            if frames:return frames.pop(0)
            await asyncio.sleep(100)
        async def send(self,data):pass
        async def close(self):pass
    class Connector:
        async def __aenter__(self):return Socket()
        async def __aexit__(self,*a):pass
    c=Collection(g,tmp_path,API(),now()+timedelta(hours=10),qa_seconds=.05,connector=lambda *a,**kw:Connector())
    result=await c.run()
    with gzip.open(tmp_path/'orderbook.jsonl.gz','rt') as f: rows=[json.loads(x) for x in f]
    assert [x['sequence'] for x in rows]==[1,2,3]
    assert rows[0]['raw']=='not json' and json.loads(rows[1]['raw'])['event_type']=='unknown'
    assert set(c.book.snapshots)=={'a','b'} and result['stop_reason']=='qa_timeout'

async def test_upload_does_not_block_other_game(monkeypatch,tmp_path):
    from nba_orderbook import collector
    import threading
    entered=threading.Event(); release=threading.Event(); progress=[]
    monkeypatch.setenv('KAGGLE_KERNEL_RUN_TYPE','Batch')
    async def fake_run(self):
        if self.game.game_id=='2':
            while not entered.is_set():await asyncio.sleep(.001)
            progress.append('B_received_during_A_upload');release.set()
        return {'stop_reason':'qa_timeout'}
    monkeypatch.setattr(collector.Collection,'run',fake_run)
    async def sports(*a):await asyncio.sleep(100)
    monkeypatch.setattr(collector,'sports_updates',sports)
    class Backend:
        def publish(self,path,ref,**kwargs):
            if ref.endswith('nba-a-b'):
                entered.set();assert release.wait(2)
            return {'status':'ready'}
    class API:
        async def close(self):pass
    a=game();b=game(gid='2');b.slug='nba-c-d'
    await collector.collect_batch({'games':[a.to_dict(),b.to_dict()],'owner':'qa','qa_seconds':1},tmp_path,Backend(),API())
    assert progress==['B_received_during_A_upload']

def test_unknown_submission_and_missed_window(tmp_path):
    from nba_orderbook.scheduler import Store,submit_locked
    from nba_orderbook.model import now,iso
    store=Store(tmp_path)
    class Backend:
        calls=0
        def list_active(self):return []
        def submit(self,*args):self.calls+=1;raise TimeoutError
    backend=Backend();g=game(iso(now()+timedelta(minutes=5)))
    b={'id':'b','state':'planned','games':[g.to_dict()],'submit_at':iso(g.window)}
    state={'batches':{'b':b}};config={'owner':'qa','cpu_notebook_capacity':4}
    submit_locked(b,store,state,config,backend)
    assert b['state']=='submission_unknown'
    submit_locked(b,store,state,config,backend);assert backend.calls==1
    g=game(iso(now()-timedelta(hours=7)))
    b={'id':'c','state':'planned','games':[g.to_dict()],'submit_at':iso(g.window)}
    submit_locked(b,store,{'batches':{'c':b}},config,backend)
    assert b['state']=='missed_window' and backend.calls==1

async def test_postponed_without_new_time_stops_subscription(tmp_path):
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(minutes=5)))
    class API:
        async def resolution(self,g):return None
        async def refresh(self,g):g.status='Postponed';return g
    c=Collection(g,tmp_path,API(),now()+timedelta(hours=10))
    await c.controls()
    assert c.reason=='postponed_waiting_schedule' and c.stop.is_set()

async def test_t10_reschedule_pauses_existing_socket(tmp_path):
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(minutes=5)))
    class API:
        async def resolution(self,g):return None
        async def refresh(self,g):g.reschedule(iso(now()+timedelta(hours=2)));return g
    c=Collection(g,tmp_path,API(),now()+timedelta(hours=10))
    class Socket:
        closed=False
        async def close(self):self.closed=True;c.stop.set()
    socket=Socket();c.socket=socket
    await c.controls()
    assert socket.closed and g.window>now()

async def test_reschedule_exceeding_budget_is_interruption(tmp_path):
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(minutes=5)))
    class API:
        async def resolution(self,g):return None
        async def refresh(self,g):g.reschedule(iso(now()+timedelta(hours=9)));return g
    c=Collection(g,tmp_path,API(),now()+timedelta(hours=10))
    await c.controls()
    assert c.reason=='rescheduled_beyond_budget' and c.resolution is None

async def test_disconnect_rebuilds_both_assets_and_records_unrecoverable_gap(tmp_path):
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(hours=2)))
    class API:
        async def resolution(self,g):return None
    attempts=[]
    class Socket:
        def __init__(self,attempt):
            self.attempt=attempt
            self.frames=[json.dumps({'event_type':'book','asset_id':asset,'bids':[{'price':'.2','size':str(attempt)}]}) for asset in ['a','b']]
            if attempt==2:self.frames.append(json.dumps({'event_type':'market_resolved','market':'condition'}))
        def __aiter__(self):return self
        async def __anext__(self):
            if self.frames:return self.frames.pop(0)
            raise ConnectionError
        async def send(self,data):pass
    class Connector:
        async def __aenter__(self):attempts.append(1);return Socket(len(attempts))
        async def __aexit__(self,*a):pass
    c=Collection(g,tmp_path,API(),now()+timedelta(hours=10),qa_seconds=10,connector=lambda *a,**kw:Connector())
    result=await c.run()
    assert len(attempts)==2
    assert c.book.assets['a']['bids']=={'0.2':'2'} and c.book.assets['b']['bids']=={'0.2':'2'}
    assert any(x['kind']=='disconnect' and x['historical_deltas_recoverable'] is False for x in result['gaps'])

def test_decimal_scale_does_not_create_or_leave_duplicate_levels():
    b=Book(['a','b'])
    b.apply({'event_type':'book','asset_id':'a','bids':[{'price':'0.10','size':'4'}]})
    b.apply({'event_type':'price_change','price_changes':[{'asset_id':'a','side':'BUY','price':'0.1','size':'2'}]})
    assert b.assets['a']['bids']=={'0.1':'2'}
    b.apply({'event_type':'price_change','price_changes':[{'asset_id':'a','side':'BUY','price':'0.100','size':'0'}]})
    assert b.assets['a']['bids']=={}

class SchedulerBackend:
    def __init__(self):self.known_sessions=[];self.rows=None
    def list_active(self):return []
    def inspect_dataset(self,ref):return None
    def catalog(self,rows,directory):self.rows=rows
    def snapshot(self,*args):return {'status':'RUNNING','version':1}

class FakeTimers:
    def __init__(self):self.scheduled=[];self.cancelled=[]
    def schedule(self,bid,*args):self.scheduled.append(bid)
    def cancel(self,bid):self.cancelled.append(bid)

async def test_new_ambiguity_cancels_old_plan_and_does_not_reuse_old_moneyline(tmp_path):
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(hours=3)))
    class API:
        ambiguous=False
        async def discover(self):
            if self.ambiguous:return [],[{'game_id':'1','reason':'multiple_moneylines','conditions':['condition','other']}]
            return [g],[]
    api=API();backend=SchedulerBackend();timers=FakeTimers()
    config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    first=await scan(config,backend,api,timers)
    assert len(timers.scheduled)==1
    api.ambiguous=True
    state=await scan(config,backend,api,timers)
    assert state['games']['1']['ambiguous_market'] is True
    assert state['anomalies'][0]['reason']=='multiple_moneylines'
    assert timers.cancelled==timers.scheduled
    assert not state['batches']
    # 第三轮无候选且无新异常，不代表上一轮歧义已经解除。
    async def empty():return [],[]
    api.discover=empty
    state=await scan(config,backend,api,timers)
    assert state['games']['1']['ambiguous_market'] is True
    assert not state['batches'] and len(timers.scheduled)==1
    # 第四轮明确选出唯一新 condition 后才重新安排。
    g.condition_id='new_unique_condition'
    async def unique():return [g],[]
    api.discover=unique
    state=await scan(config,backend,api,timers)
    assert state['games']['1']['ambiguous_market'] is False
    assert len(timers.scheduled)==2
    assert next(iter(state['batches'].values()))['games'][0]['condition_id']=='new_unique_condition'

async def test_active_schedule_updates_but_catalog_uses_final_published_time(tmp_path):
    from nba_orderbook.scheduler import scan,Store
    from nba_orderbook.model import now,iso
    old=game(iso(now()+timedelta(hours=3)))
    changed=game(iso(now()+timedelta(hours=4)))
    final=iso(now()+timedelta(hours=5))
    store=Store(tmp_path)
    store.save({'games':{'1':{'game':old.to_dict(),'batch_id':'active'}},'batches':{
        'active':{'id':'active','state':'submitted','ref':'qa/nba-batch-active','version':1,'games':[old.to_dict()]}},'anomalies':[]})
    class API:
        async def discover(self):return [changed],[]
    class Backend(SchedulerBackend):
        def inspect_dataset(self,ref):return {'status':'READY','version':1,'summary':{'stop_reason':'market_resolved','quality':'partial'}}
        def read_json(self,ref,name,version):assert name=='metadata.json';return {'start':final}
    backend=Backend();timers=FakeTimers()
    state=await scan({'state_dir':str(tmp_path),'owner':'qa','_path':'unused'},backend,API(),timers)
    assert state['games']['1']['game']['start']==changed.start
    assert state['batches']['active']['games'][0]['start']==old.start
    assert backend.rows[0]['game_start']==final
    assert not timers.scheduled

async def test_delayed_later_game_uses_first_actual_message_and_two_book_coverage(monkeypatch,tmp_path):
    from nba_orderbook import collector
    g=game('2026-11-18T03:00:00Z')
    clock=[g.window]
    monkeypatch.setattr(collector,'now',lambda:clock[0])
    class API:
        async def resolution(self,g):return None
    class Socket:
        def __init__(self):self.n=0
        def __aiter__(self):return self
        async def __anext__(self):
            self.n+=1
            if self.n==1:
                clock[0]=dt(g.start)-timedelta(minutes=2)
                return json.dumps({'event_type':'book','asset_id':'a'})
            if self.n==2:
                clock[0]=dt(g.start)+timedelta(seconds=2)
                return json.dumps({'event_type':'book','asset_id':'b'})
            return json.dumps({'event_type':'market_resolved','market':'condition'})
        async def send(self,raw):pass
    class Connector:
        async def __aenter__(self):return Socket()
        async def __aexit__(self,*args):pass
    c=Collection(g,tmp_path,API(),g.deadline+timedelta(hours=1),connector=lambda *a,**kw:Connector())
    result=await c.run()
    assert dt(result['runtime_started_at'])==g.window
    assert dt(result['started_at'])==dt(g.start)-timedelta(minutes=2)
    assert dt(result['coverage_started_at'])==dt(g.start)+timedelta(seconds=2)
    assert result['on_time_snapshots'] is False and result['quality']=='partial'
    assert any(x['kind']=='late_start' for x in result['gaps'])


async def test_cached_unique_game_without_current_candidate_keeps_discovering(tmp_path):
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(hours=3)))
    class API:
        current=True
        async def discover(self):return ([g] if self.current else []),[]
    api=API();backend=SchedulerBackend();timers=FakeTimers()
    config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    await scan(config,backend,api,timers)
    api.current=False
    state=await scan(config,backend,api,timers)
    assert not state['batches'] and len(timers.scheduled)==1
    api.current=True
    state=await scan(config,backend,api,timers)
    assert len(state['batches'])==1 and len(timers.scheduled)==2

async def test_schedule_change_after_unchanged_t10_is_rechecked_and_pauses(monkeypatch,tmp_path):
    from nba_orderbook import collector
    g=game('2026-11-18T03:00:00Z');original=g.start
    clock=[dt(g.start)-timedelta(minutes=10)];calls=[]
    monkeypatch.setattr(collector,'now',lambda:clock[0])
    class API:
        async def resolution(self,g):return None
        async def refresh(self,g):
            calls.append(clock[0])
            if len(calls)==2:g.reschedule('2026-11-18T05:00:00Z')
            return g
    c=Collection(g,tmp_path,API(),dt('2026-11-18T15:00:00Z'))
    class Socket:
        closed=False
        async def close(self):self.closed=True
    socket=Socket();c.socket=socket
    async def step(seconds):
        if len(calls)==2:c.stop.set()
        else:clock[0]+=timedelta(minutes=5)
    monkeypatch.setattr(collector.asyncio,'sleep',step)
    await c.controls()
    assert calls==[dt(original)-timedelta(minutes=10),dt(original)-timedelta(minutes=5)]
    assert socket.closed and g.window>clock[0]
    assert g.deadline==dt('2026-11-18T11:00:00Z')
    assert c.next_schedule_check<=dt(g.start)-timedelta(minutes=10)

async def test_late_reschedule_crossing_budget_is_seen_after_t10(monkeypatch,tmp_path):
    from nba_orderbook import collector
    g=game('2026-11-18T03:00:00Z');clock=[dt(g.start)-timedelta(minutes=10)];calls=[]
    monkeypatch.setattr(collector,'now',lambda:clock[0])
    class API:
        async def resolution(self,g):return None
        async def refresh(self,g):
            calls.append(clock[0])
            if len(calls)==2:g.reschedule('2026-11-18T10:00:00Z')
            return g
    async def step(seconds):clock[0]+=timedelta(minutes=5)
    monkeypatch.setattr(collector.asyncio,'sleep',step)
    c=Collection(g,tmp_path,API(),dt('2026-11-18T15:00:00Z'))
    await c.controls()
    assert len(calls)==2 and c.reason=='rescheduled_beyond_budget'

async def test_observed_start_ends_schedule_refresh_and_freezes_deadline(monkeypatch,tmp_path):
    from nba_orderbook import collector
    g=game('2026-11-18T03:00:00Z');clock=[dt(g.start)-timedelta(minutes=10)];calls=[]
    monkeypatch.setattr(collector,'now',lambda:clock[0])
    class API:
        async def resolution(self,g):return None
        async def refresh(self,g):
            calls.append(clock[0]);g.reschedule(g.start,live=True);return g
    c=Collection(g,tmp_path,API(),dt('2026-11-18T15:00:00Z'))
    async def step(seconds):
        clock[0]+=timedelta(minutes=5)
        if clock[0]>=dt(g.start):c.stop.set()
    monkeypatch.setattr(collector.asyncio,'sleep',step)
    await c.controls()
    g.reschedule('2026-11-19T03:00:00Z')
    assert len(calls)==1 and g.deadline==dt('2026-11-18T09:00:00Z')

async def test_application_pong_deadline_reconnects_and_normal_pongs_are_retained(monkeypatch,tmp_path):
    from nba_orderbook import collector
    from nba_orderbook.model import now,iso
    monkeypatch.setattr(collector,'HEARTBEAT_INTERVAL',.005)
    monkeypatch.setattr(collector,'PONG_TIMEOUT',.02)
    class API:
        async def resolution(self,g):return None
    for responding in (False,True):
        attempts=[];sockets=[]
        class Socket:
            def __init__(self,attempt):
                self.queue=asyncio.Queue();self.closed=False;self.pings=0
                for asset in ('a','b'):
                    self.queue.put_nowait(json.dumps({'event_type':'book','asset_id':asset,'bids':[{'price':'.2','size':str(attempt)}]}))
                if attempt==2:self.queue.put_nowait(json.dumps({'event_type':'market_resolved','market':'condition'}))
            def __aiter__(self):return self
            async def __anext__(self):
                value=await self.queue.get()
                if value is None:raise StopAsyncIteration
                return value
            async def send(self,raw):
                if raw=='PING':
                    self.pings+=1
                    if responding:
                        self.queue.put_nowait('PONG')
                        if self.pings==3:self.queue.put_nowait(json.dumps({'event_type':'market_resolved','market':'condition'}))
            async def close(self):self.closed=True;self.queue.put_nowait(None)
        class Connector:
            async def __aenter__(self):
                attempts.append(1);socket=Socket(len(attempts));sockets.append(socket);return socket
            async def __aexit__(self,*args):pass
        g=game(iso(now()+timedelta(hours=2)))
        directory=tmp_path/str(responding)
        c=Collection(g,directory,API(),now()+timedelta(hours=10),qa_seconds=10,connector=lambda *a,**kw:Connector())
        result=await c.run()
        with gzip.open(directory/'orderbook.jsonl.gz','rt') as stream:rows=[json.loads(line) for line in stream]
        if responding:
            assert len(attempts)==1 and sockets[0].closed is False
            assert sum(row['raw']=='PONG' for row in rows)==3
            assert not any(gap['kind']=='heartbeat_timeout' for gap in result['gaps'])
        else:
            assert len(attempts)==2 and sockets[0].closed
            assert any(gap['kind']=='heartbeat_timeout' for gap in result['gaps'])
            assert any(gap['kind']=='disconnect' for gap in result['gaps'])
            assert c.book.assets['a']['bids']=={'0.2':'2'} and c.book.assets['b']['bids']=={'0.2':'2'}
            assert len({row['connection_id'] for row in rows})==2

async def test_heartbeat_send_failure_closes_connection(monkeypatch,tmp_path):
    from nba_orderbook import collector
    from nba_orderbook.model import now,iso
    monkeypatch.setattr(collector,'HEARTBEAT_INTERVAL',.001)
    class Socket:
        closed=False
        async def send(self,raw):raise RuntimeError('transport write failure')
        async def close(self):self.closed=True
    c=Collection(game(iso(now())),tmp_path,None,now()+timedelta(hours=10))
    socket=Socket()
    await c.heartbeat(socket,asyncio.Event())
    assert socket.closed and c.gaps[0]['kind']=='heartbeat_error'

async def test_postponed_with_new_time_survives_later_scans_and_new_untimed_episode_waits(tmp_path):
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    original=game(iso(now()+timedelta(hours=3)))
    revised=iso(now()+timedelta(hours=4))
    class API:
        round=0
        async def discover(self):
            current=Game.from_dict(original.to_dict())
            if self.round:
                current.start=revised
                current.status='Scheduled' if self.round==3 else 'Postponed'
                current.condition_id='current-condition-'+str(self.round)
                current.assets=['current-a-'+str(self.round),'current-b-'+str(self.round)]
            return [current],[]
    api=API();backend=SchedulerBackend();timers=FakeTimers()
    config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    for round in range(5):
        api.round=round
        state=await scan(config,backend,api,timers)
        row=state['games']['1']
        if round in (1,2):
            assert row['game']['postponed_with_time'] is True
            assert row['game']['condition_id']=='current-condition-'+str(round)
            assert row['game']['assets']==['current-a-'+str(round),'current-b-'+str(round)]
            assert row['waiting_schedule'] is False and len(state['batches'])==1
        if round==3:assert row['game']['postponed_with_time'] is False
    assert state['games']['1']['waiting_schedule'] is True
    assert not state['batches']

async def test_collector_keeps_accepted_postponed_time_and_rechecks_new_t10(monkeypatch,tmp_path):
    from nba_orderbook import collector
    g=game('2026-11-18T03:00:00Z');clock=[dt(g.start)-timedelta(minutes=10)];calls=[]
    monkeypatch.setattr(collector,'now',lambda:clock[0])
    class API:
        async def resolution(self,g):return None
        async def refresh(self,g):
            calls.append(clock[0])
            g.reschedule('2026-11-18T05:00:00Z','Scheduled' if len(calls)==4 else 'Postponed')
            return g
    async def step(seconds):
        if len(calls)==2:clock[0]=dt('2026-11-18T04:50:00Z')
        else:clock[0]+=timedelta(minutes=5)
    monkeypatch.setattr(collector.asyncio,'sleep',step)
    c=Collection(g,tmp_path,API(),dt('2026-11-18T15:00:00Z'))
    class Socket:
        async def close(self):pass
    c.socket=Socket()
    await c.controls()
    assert len(calls)==5 and calls[2]==dt('2026-11-18T04:50:00Z')
    assert len(g.time_changes)==1
    assert c.reason=='postponed_waiting_schedule' and g.postponed_with_time is False

async def test_not_attempted_submission_keeps_batch_id_across_reschedule_and_retry(monkeypatch,tmp_path):
    from nba_orderbook import scheduler
    from nba_orderbook.kaggle import SubmissionNotAttempted
    from nba_orderbook.model import now,iso
    clock=[now()];monkeypatch.setattr(scheduler,'now',lambda:clock[0])
    current=game(iso(clock[0]+timedelta(minutes=5)))
    class API:
        async def discover(self):return [Game.from_dict(current.to_dict())],[]
    class Backend(SchedulerBackend):
        def __init__(self):super().__init__();self.attempts=[]
        def submit(self,batch,root):
            self.attempts.append((batch['id'],batch['ref']))
            if len(self.attempts)==1:raise SubmissionNotAttempted('read failed before SaveKernel')
            return {'ref':batch['ref'],'version':1}
    backend=Backend();timers=FakeTimers();config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    first=await scheduler.scan(config,backend,API(),timers)
    bid=next(iter(first['batches']))
    assert first['batches'][bid]['state']=='planned'
    current.start=iso(clock[0]+timedelta(hours=2))
    second=await scheduler.scan(config,backend,API(),timers)
    assert list(second['batches'])==[bid] and len(backend.attempts)==1
    assert timers.scheduled==[bid]
    clock[0]=current.window
    third=await scheduler.scan(config,backend,API(),timers)
    assert third['batches'][bid]['state']=='submitted'
    assert len(backend.attempts)==2 and backend.attempts[0]==backend.attempts[1]

def test_unexpected_submission_exception_stays_unknown(tmp_path):
    from nba_orderbook.scheduler import Store,submit_locked
    from nba_orderbook.model import now,iso
    class Backend(SchedulerBackend):
        calls=0
        def submit(self,*args):self.calls+=1;raise ValueError('after possible SaveKernel')
    backend=Backend();g=game(iso(now()+timedelta(minutes=5)))
    batch={'id':'fixed','state':'planned','games':[g.to_dict()],'submit_at':iso(g.window)}
    state={'batches':{'fixed':batch}};store=Store(tmp_path)
    submit_locked(batch,store,state,{'owner':'qa'},backend)
    submit_locked(batch,store,state,{'owner':'qa'},backend)
    assert batch['state']=='submission_unknown' and backend.calls==1


def test_old_game_state_without_postponement_field_remains_readable():
    old=game().to_dict();old.pop('postponed_with_time')
    restored=Game.from_dict(old)
    assert restored.postponed_with_time is False
    restored.reschedule(restored.start,'Postponed')
    assert restored.postponed_with_time is False

async def test_retryable_batch_waits_through_missing_and_ambiguous_discovery(tmp_path):
    from nba_orderbook import scheduler
    from nba_orderbook.kaggle import SubmissionNotAttempted
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(minutes=5)))
    class API:
        round=0
        async def discover(self):
            if self.round==1:return [],[{'game_id':'1','reason':'multiple_moneylines'}]
            if self.round==2:return [],[]
            return [Game.from_dict(g.to_dict())],[]
    class Backend(SchedulerBackend):
        def __init__(self):super().__init__();self.ids=[]
        def submit(self,batch,root):
            self.ids.append(batch['id'])
            if len(self.ids)==1:raise SubmissionNotAttempted('prewrite read failed')
            return {'ref':batch['ref'],'version':1}
    api=API();backend=Backend();config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    for round in range(4):
        api.round=round
        state=await scheduler.scan(config,backend,api,FakeTimers())
        assert len(state['batches'])==1
        if round in (1,2):assert len(backend.ids)==1
    assert len(backend.ids)==2 and backend.ids[0]==backend.ids[1]

async def test_unknown_submission_never_rebuilds_batch_during_later_scan(tmp_path):
    from nba_orderbook import scheduler
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(minutes=5)))
    class API:
        async def discover(self):return [Game.from_dict(g.to_dict())],[]
    class Backend(SchedulerBackend):
        def __init__(self):super().__init__();self.calls=0
        def submit(self,batch,root):self.calls+=1;raise ValueError('write may have happened')
        def snapshot(self,*args):raise RuntimeError('still cannot confirm remote')
    backend=Backend();config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    first=await scheduler.scan(config,backend,API(),FakeTimers())
    bid=next(iter(first['batches']))
    second=await scheduler.scan(config,backend,API(),FakeTimers())
    assert list(second['batches'])==[bid] and second['batches'][bid]['state']=='submission_unknown'
    assert backend.calls==1

async def test_prewrite_retry_is_regrouped_before_save_after_game_moves_beyond_two_hours(monkeypatch,tmp_path):
    from nba_orderbook import scheduler
    from nba_orderbook.kaggle import SubmissionNotAttempted
    from nba_orderbook.model import now,iso
    clock=now();monkeypatch.setattr(scheduler,'now',lambda:clock)
    a=game(iso(clock+timedelta(minutes=60)),'A');a.slug='nba-a'
    b=game(iso(clock+timedelta(minutes=120)),'B');b.slug='nba-b'
    class API:
        async def discover(self):return [Game.from_dict(g.to_dict()) for g in (a,b)],[]
    class Backend(SchedulerBackend):
        def __init__(self):super().__init__();self.attempts=[]
        def submit(self,batch,root):
            self.attempts.append({'id':batch['id'],'ref':batch['ref'],'games':[g.copy() for g in batch['games']]})
            if len(self.attempts)==1:raise SubmissionNotAttempted('no SaveKernel before failed list')
            return {'ref':batch['ref'],'version':1}
    backend=Backend();timers=FakeTimers();config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused','cpu_notebook_capacity':5}
    first=await scheduler.scan(config,backend,API(),timers)
    original=next(iter(first['batches']))
    assert first['batches'][original]['state']=='planned'
    assert len(first['batches'][original]['games'])==2
    b.start=iso(clock+timedelta(hours=5))
    second=await scheduler.scan(config,backend,API(),timers)
    assert second['batches'][original]['state']=='submitted'
    assert [g['game_id'] for g in second['batches'][original]['games']]==['A']
    remaining=[batch for bid,batch in second['batches'].items() if bid!=original]
    assert len(remaining)==1 and remaining[0]['state']=='planned'
    assert [g['game_id'] for g in remaining[0]['games']]==['B']
    assert remaining[0]['id'] in timers.scheduled
    assert second['games']['B']['batch_id']==remaining[0]['id']
    assert backend.attempts[0]['id']==backend.attempts[1]['id']==original
    assert backend.attempts[0]['ref']==backend.attempts[1]['ref']
    assert second['required_peak_notebooks']==2
    for saved in backend.attempts[1:]:
        starts=[dt(g['start']) for g in saved['games']]
        assert max(starts)-min(starts)<=timedelta(hours=2)
    # 下一轮不能丢失脱离原重试组的B，已经提交的A也不能被重新分组。
    third=await scheduler.scan(config,backend,API(),timers)
    assert third['batches'][original]['games']==second['batches'][original]['games']
    assigned=third['games']['B']['batch_id']
    assert third['batches'][assigned]['state']=='planned' and assigned in timers.scheduled
    assert third['required_peak_notebooks']==2 and len(backend.attempts)==2
