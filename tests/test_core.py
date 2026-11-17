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
    assert not any(b['state']=='planned' for b in state['batches'].values())
    # 第三轮无候选且无新异常，不代表上一轮歧义已经解除。
    async def empty():return [],[]
    api.discover=empty
    state=await scan(config,backend,api,timers)
    assert state['games']['1']['ambiguous_market'] is True
    assert not any(b['state']=='planned' for b in state['batches'].values()) and len(timers.scheduled)==1
    # 第四轮明确选出唯一新 condition 后才重新安排。
    g.condition_id='new_unique_condition'
    async def unique():return [g],[]
    api.discover=unique
    state=await scan(config,backend,api,timers)
    assert state['games']['1']['ambiguous_market'] is False
    assert len(timers.scheduled)==2
    assert next(b for b in state['batches'].values() if b['state']=='planned')['games'][0]['condition_id']=='new_unique_condition'

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
        def inspect_dataset(self,ref):return {'status':'READY','version':1,'files':['orderbook.jsonl','metadata.json','summary.json'],'summary':{'slug':old.slug,'condition_id':old.condition_id,'game_start':final,'stop_reason':'market_resolved','quality':'partial'}}
        def read_json(self,ref,name,version):assert name=='metadata.json';return {**old.to_dict(),'start':final}
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
    assert not any(b['state']=='planned' for b in state['batches'].values()) and len(timers.scheduled)==1
    api.current=True
    state=await scan(config,backend,api,timers)
    assert sum(b['state']=='planned' for b in state['batches'].values())==1 and len(timers.scheduled)==2

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
            assert row['waiting_schedule'] is False and sum(b['state']=='planned' for b in state['batches'].values())==1
        if round==3:assert row['game']['postponed_with_time'] is False
    assert state['games']['1']['waiting_schedule'] is True
    assert not any(b['state']=='planned' for b in state['batches'].values())

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

async def test_deleted_ended_notebook_does_not_block_future_planning(tmp_path):
    from nba_orderbook.scheduler import scan,Store
    from nba_orderbook.model import now,iso
    future=game(iso(now()+timedelta(hours=3)),'future')
    Store(tmp_path).save({'games':{},'batches':{'old':{'id':'old','state':'ended','ref':'qa/deleted','version':1}},'anomalies':[]})
    class API:
        async def discover(self):return [future],[]
    class Backend(SchedulerBackend):
        def __init__(self):super().__init__();self.known_sessions=[{'ref':'qa/deleted','version':1}]
        def list_active(self):
            assert {'ref':'qa/deleted','version':1} not in self.known_sessions
            return []
    backend=Backend();timers=FakeTimers()
    state=await scan({'state_dir':str(tmp_path),'owner':'qa','_path':'unused'},backend,API(),timers)
    bid=state['games']['future']['batch_id']
    assert state['batches'][bid]['state']=='planned' and bid in timers.scheduled
    assert backend.known_sessions==[]

async def test_active_and_unknown_notebook_lookup_errors_are_not_free_capacity(tmp_path):
    import pytest,requests
    from nba_orderbook.scheduler import scan,Store
    from nba_orderbook.model import now,iso
    future=game(iso(now()+timedelta(hours=3)),'future')
    old=game(iso(now()+timedelta(minutes=30)),'old')
    class API:
        async def discover(self):return [future],[]
    for status in ('submitted','submission_unknown'):
        path=tmp_path/status
        Store(path).save({'games':{},'batches':{'old':{'id':'old','state':status,'ref':'qa/unconfirmed','version':1,'games':[old.to_dict()]}},'anomalies':[]})
        class Backend(SchedulerBackend):
            def list_active(self):
                assert {'ref':'qa/unconfirmed','version':1} in self.known_sessions
                response=requests.Response();response.status_code=403
                raise requests.HTTPError('cannot verify registered version',response=response)
        backend=Backend();timers=FakeTimers()
        failed=await scan({'state_dir':str(path),'owner':'qa','_path':'unused'},backend,API(),timers)
        assert failed['occupancy_error']=='HTTPError' and failed['observed_active_notebooks'] is None
        assert timers.scheduled==[]
        assert {'ref':'qa/unconfirmed','version':1} in backend.known_sessions

async def test_reused_backend_removes_version_only_after_terminal_confirmation(tmp_path):
    from nba_orderbook.scheduler import scan,Store
    from nba_orderbook.model import now,iso
    old=game(iso(now()+timedelta(minutes=30)),'old')
    Store(tmp_path).save({'games':{},'batches':{'old':{'id':'old','state':'submitted','ref':'qa/old','version':1,'games':[old.to_dict()]}},'anomalies':[]})
    class API:
        async def discover(self):return [],[]
    class Backend(SchedulerBackend):
        terminal=False;deleted=False
        def __init__(self):
            super().__init__();self.known_sessions=[{'ref':'qa/old','version':1},{'ref':'qa/other-project','version':7}]
        def list_active(self):
            if self.deleted:assert {'ref':'qa/old','version':1} not in self.known_sessions
            else:assert {'ref':'qa/old','version':1} in self.known_sessions
            assert {'ref':'qa/other-project','version':7} in self.known_sessions
            return []
        def snapshot(self,*args):return {'status':'COMPLETE' if self.terminal else 'RUNNING','version':1}
    backend=Backend();config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    first=await scan(config,backend,API(),FakeTimers())
    assert first['batches']['old']['state']=='submitted'
    assert backend.known_sessions.count({'ref':'qa/old','version':1})==1
    backend.terminal=True
    second=await scan(config,backend,API(),FakeTimers())
    assert second['batches']['old']['state']=='ended'
    assert backend.known_sessions==[{'ref':'qa/other-project','version':7}]
    backend.deleted=True
    third=await scan(config,backend,API(),FakeTimers())
    assert third['batches']['old']['state']=='ended'
    assert backend.known_sessions==[{'ref':'qa/other-project','version':7}]

async def test_real_backend_explicit_submission_rejection_recovers_same_reference_next_scan(tmp_path):
    from types import SimpleNamespace as N
    from nba_orderbook.kaggle import Backend,MARKER
    from kagglesdk.kernels.types.kernels_api_service import ApiSaveKernelResponse
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    root=tmp_path/'package';(root/'src/nba_orderbook').mkdir(parents=True)
    (root/'requirements.txt').write_text('kagglesdk==0.1.38\n')
    g=game(iso(now()+timedelta(minutes=60)))
    class API:
        async def discover(self):return [Game.from_dict(g.to_dict())],[]
    class Kernels:
        def __init__(self):self.requests=[];self.accepted=None
        def list_kernels(self,request):
            return N(kernels=[N(ref=self.accepted)] if self.accepted else [],next_page_token='')
        def save_kernel(self,request):
            self.requests.append(request)
            if len(self.requests)==1:
                response=ApiSaveKernelResponse();response.error='CPU batch capacity reached'
                return response
            self.accepted=request.slug
            response=ApiSaveKernelResponse();response.version_number=1
            return response
        def get_kernel(self,request):
            bid=self.accepted.split('nba-batch-',1)[1]
            return N(metadata=N(current_version_number=1,enable_gpu=False,enable_tpu=False),blob=N(source=json.dumps({'cells':[{'source':MARKER+bid}]})))
        def get_kernel_session_status(self,request):return N(status=N(name='RUNNING'))
    kernels=Kernels();backend=Backend('qa',client=N(kernels=N(kernels_api_client=kernels)))
    backend.inspect_dataset=lambda ref:None
    config={'state_dir':str(tmp_path/'state'),'owner':'qa','_path':'unused','source_dir':str(root)}
    first=await scan(config,backend,API(),FakeTimers())
    bid=next(iter(first['batches']))
    assert first['batches'][bid]['state']=='planned' and first['batches'][bid]['error']=='SubmissionRejected'
    second=await scan(config,backend,API(),FakeTimers())
    assert list(second['batches'])==[bid] and second['batches'][bid]['state']=='submitted'
    assert len(kernels.requests)==2 and kernels.requests[0].slug==kernels.requests[1].slug

async def test_real_backend_rejected_response_with_failed_absence_check_is_not_retried(tmp_path):
    import requests
    from types import SimpleNamespace as N
    from nba_orderbook.kaggle import Backend
    from kagglesdk.kernels.types.kernels_api_service import ApiSaveKernelResponse
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    root=tmp_path/'package';(root/'src/nba_orderbook').mkdir(parents=True)
    (root/'requirements.txt').write_text('kagglesdk==0.1.38\n')
    g=game(iso(now()+timedelta(minutes=60)))
    class API:
        async def discover(self):return [Game.from_dict(g.to_dict())],[]
    class Kernels:
        writes=0;fail_list=False
        def list_kernels(self,request):
            if self.fail_list:raise requests.Timeout('cannot prove ref absent')
            return N(kernels=[],next_page_token='')
        def save_kernel(self,request):
            self.writes+=1;self.fail_list=True
            response=ApiSaveKernelResponse();response.error='CPU batch capacity reached'
            return response
        def get_kernel(self,request):raise requests.Timeout('remote status remains unknown')
    kernels=Kernels();backend=Backend('qa',client=N(kernels=N(kernels_api_client=kernels)))
    backend.inspect_dataset=lambda ref:None
    config={'state_dir':str(tmp_path/'state'),'owner':'qa','_path':'unused','source_dir':str(root)}
    first=await scan(config,backend,API(),FakeTimers())
    bid=next(iter(first['batches']))
    assert first['batches'][bid]['state']=='submission_unknown'
    kernels.fail_list=False
    second=await scan(config,backend,API(),FakeTimers())
    assert list(second['batches'])==[bid] and second['batches'][bid]['state']=='submission_unknown'
    assert kernels.writes==1

class TrackingTimers(FakeTimers):
    def __init__(self):super().__init__();self.active=set()
    def schedule(self,bid,*args):super().schedule(bid,*args);self.active.add(bid)
    def cancel(self,bid):super().cancel(bid);self.active.discard(bid)

async def test_occupancy_timeout_preserves_valid_existing_future_timer_and_recovers(tmp_path):
    import requests
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(hours=3)))
    class API:
        async def discover(self):return [Game.from_dict(g.to_dict())],[]
    class Backend(SchedulerBackend):
        timeout=False
        def list_active(self):
            if self.timeout:raise requests.Timeout('capacity read failed')
            return []
    backend=Backend();timers=TrackingTimers();config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    first=await scan(config,backend,API(),timers);bid=first['games']['1']['batch_id']
    assert timers.active=={bid}
    backend.timeout=True
    second=await scan(config,backend,API(),timers)
    assert timers.active=={bid} and timers.cancelled==[]
    assert second['batches'][bid]['state']=='planned' and second['games']['1']['batch_id']==bid
    assert second['observed_active_notebooks'] is None and second['occupancy_error']=='Timeout'
    assert json.loads((tmp_path/'state.json').read_text())['occupancy_error']=='Timeout'
    backend.timeout=False
    third=await scan(config,backend,API(),timers)
    assert 'occupancy_error' not in third and third['observed_active_notebooks']==[]
    assert third['games']['1']['batch_id'] in timers.active

async def test_invalid_old_timer_is_persistently_blocked_before_cancel_even_when_capacity_fails(tmp_path):
    import requests
    from nba_orderbook.scheduler import scan,submit
    from nba_orderbook.model import now,iso
    for failure in ('missing','ambiguous','postponed','rescheduled'):
        directory=tmp_path/failure
        g=game(iso(now()+timedelta(hours=3)))
        class API:
            invalid=False;restored=False
            async def discover(self):
                if self.invalid and not self.restored:
                    if failure=='missing':return [],[]
                    if failure=='ambiguous':return [],[{'game_id':'1','reason':'multiple_moneylines'}]
                current=Game.from_dict(g.to_dict())
                if self.invalid and failure=='rescheduled':current.start=iso(dt(g.start)+timedelta(hours=1))
                if self.invalid and not self.restored and failure=='postponed':current.status='Postponed'
                return [current],[]
        class Backend(SchedulerBackend):
            timeout=False;writes=0
            def list_active(self):
                if self.timeout:raise requests.Timeout('capacity read failed')
                return []
            def submit(self,*args):self.writes+=1;raise AssertionError('old invalid timer must never submit')
        class Timers(TrackingTimers):
            enforce=False
            def cancel(self,bid):
                if self.enforce:
                    durable=json.loads((directory/'state.json').read_text())
                    assert durable['batches'][bid]['state']=='invalidated'
                super().cancel(bid)
        api=API();backend=Backend();timers=Timers();config={'state_dir':str(directory),'owner':'qa','_path':'unused'}
        first=await scan(config,backend,api,timers);bid=first['games']['1']['batch_id']
        timers.enforce=True;api.invalid=True;backend.timeout=True
        second=await scan(config,backend,api,timers)
        assert second['batches'][bid]['state']=='invalidated' and bid not in timers.active
        assert submit(config,backend,bid)['state']=='invalidated'
        assert backend.writes==0 and second['observed_active_notebooks'] is None
        timers.enforce=False;api.restored=True;backend.timeout=False
        third=await scan(config,backend,api,timers)
        new_bid=third['games']['1']['batch_id']
        assert new_bid!=bid and new_bid in timers.active
        assert third['batches'][new_bid]['games'][0]['start']==(iso(dt(g.start)+timedelta(hours=1)) if failure=='rescheduled' else g.start)
        assert 'occupancy_error' not in third

async def test_due_capacity_failure_does_not_lose_other_future_group_timer(tmp_path):
    import requests
    from nba_orderbook.scheduler import scan,submit
    from nba_orderbook.model import now,iso
    due=game(iso(now()+timedelta(minutes=5)),'due')
    future=game(iso(now()+timedelta(hours=5)),'future')
    class API:
        async def discover(self):return [due,future],[]
    class Backend(SchedulerBackend):
        reads=0;writes=0
        def list_active(self):
            self.reads+=1
            if self.reads==2:raise requests.Timeout('due batch capacity lookup failed')
            return []
        def submit(self,batch,root):self.writes+=1;return {'ref':batch['ref'],'version':1}
    backend=Backend();timers=TrackingTimers();config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    state=await scan(config,backend,API(),timers)
    due_bid=state['games']['due']['batch_id'];future_bid=state['games']['future']['batch_id']
    assert backend.writes==0 and state['batches'][due_bid]['capacity_error']=='Timeout'
    assert state['batches'][due_bid]['state']=='planned' and future_bid in timers.active
    restored=submit(config,backend,due_bid)
    assert restored['state']=='submitted' and 'capacity_error' not in restored and backend.writes==1


async def test_fifty_three_unsubmitted_future_games_schedule_fifteen_batches_without_dataset_calls(tmp_path):
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    games=[]
    start=now()+timedelta(hours=3)
    for group in range(15):
        for member in range(11 if group==14 else 3):
            g=game(iso(start+timedelta(hours=3*group,minutes=10*member)),str(len(games)))
            g.slug='nba-future-'+g.game_id;games.append(g)
    class API:
        async def discover(self):return games,[]
    class Backend(SchedulerBackend):
        def inspect_dataset(self,ref):raise AssertionError('unsubmitted future game must not query Dataset')
        def catalog(self,*args):raise AssertionError('no publication to add')
    timers=TrackingTimers()
    state=await scan({'state_dir':str(tmp_path),'owner':'qa','_path':'unused'},Backend(),API(),timers)
    assert len(state['games'])==53 and len(timers.active)==15
    assert sum(b['state']=='planned' for b in state['batches'].values())==15

class PublicationBackend(SchedulerBackend):
    def __init__(self,games):
        super().__init__();self.games={g.slug:g for g in games};self.inspects=[];self.metadata_reads=[];self.catalog_calls=[]
    def inspect_dataset(self,ref):
        self.inspects.append(ref);g=self.games[ref.split('/',1)[1]]
        return {'status':'READY','version':1,'files':['orderbook.jsonl','metadata.json','summary.json'],
                'summary':{'slug':g.slug,'condition_id':g.condition_id,'game_start':g.start,'stop_reason':'market_resolved','quality':'partial'}}
    def read_json(self,ref,name,version):
        self.metadata_reads.append((ref,name,version));return self.games[ref.split('/',1)[1]].to_dict()
    def catalog(self,rows,directory):self.catalog_calls.append(rows);return {'changed':True}

def publication_state(path,games,status='ended'):
    from nba_orderbook.scheduler import Store
    state={'games':{},'batches':{},'anomalies':[]}
    for g in games:
        bid='batch-'+g.game_id
        state['games'][g.game_id]={'game':g.to_dict(),'batch_id':bid}
        state['batches'][bid]={'id':bid,'state':status,'games':[g.to_dict()],'ref':'qa/nba-'+bid,'version':1}
    Store(path).save(state)

async def test_final_publication_is_probed_once_and_cached_across_scans_with_published_time(tmp_path):
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(hours=3)))
    publication_state(tmp_path,[g])
    final=Game.from_dict(g.to_dict());final.start=iso(dt(g.start)+timedelta(hours=1))
    class API:
        async def discover(self):return [Game.from_dict(g.to_dict())],[]
    backend=PublicationBackend([final]);config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    first=await scan(config,backend,API(),TrackingTimers())
    assert backend.inspects==['qa/'+g.slug] and len(backend.metadata_reads)==1
    assert first['games'][g.game_id]['publication']['game_start']==final.start
    assert backend.catalog_calls[0][0]['final_version']==1
    # 新适配器和重新读取的旧运行状态也复用固定最终版本，不重复探测历史Dataset。
    second_backend=PublicationBackend([final])
    second=await scan(config,second_backend,API(),TrackingTimers())
    assert second_backend.inspects==[] and second_backend.metadata_reads==[]
    assert second_backend.catalog_calls[0][0]['game_start']==final.start
    assert second['games'][g.game_id]['publication']==first['games'][g.game_id]['publication']

async def test_absence_processing_or_inconsistent_files_are_not_cached(tmp_path):
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    class API:
        async def discover(self):return [],[]
    g=game(iso(now()+timedelta(hours=3)))
    for mode in ('absent','processing','missing_file','wrong_identity','wrong_time'):
        path=tmp_path/mode;publication_state(path,[g])
        class Backend(PublicationBackend):
            def inspect_dataset(self,ref):
                result=super().inspect_dataset(ref)
                if mode=='absent':return None
                if mode=='processing':result['status']='PROCESSING'
                if mode=='missing_file':result['files'].remove('orderbook.jsonl')
                if mode=='wrong_identity':result['summary']['condition_id']='other'
                if mode=='wrong_time':result['summary']['game_start']=iso(dt(g.start)+timedelta(hours=1))
                return result
        backend=Backend([g]);config={'state_dir':str(path),'owner':'qa','_path':'unused'}
        for _ in range(2):
            state=await scan(config,backend,API(),TrackingTimers())
            assert not state['games'][g.game_id].get('publication')
        assert len(backend.inspects)==2 and backend.catalog_calls==[]

async def test_rate_limit_stops_remaining_publication_queries_and_respects_retry_after(monkeypatch,tmp_path):
    import requests
    from nba_orderbook import scheduler
    from nba_orderbook.model import now,iso
    clock=[now()];monkeypatch.setattr(scheduler,'now',lambda:clock[0])
    games=[game(iso(clock[0]+timedelta(hours=3)),str(i)) for i in range(3)]
    for g in games:g.slug='nba-published-'+g.game_id
    publication_state(tmp_path,games)
    class API:
        async def discover(self):return [],[]
    class Backend(PublicationBackend):
        throttle=True
        def inspect_dataset(self,ref):
            if ref.endswith('-1') and self.throttle:
                self.inspects.append(ref)
                response=requests.Response();response.status_code=429;response.headers['Retry-After']='3600'
                raise requests.HTTPError('rate limited',response=response)
            return super().inspect_dataset(ref)
    backend=Backend(games);config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    first=await scheduler.scan(config,backend,API(),TrackingTimers())
    assert backend.inspects==['qa/nba-published-0','qa/nba-published-1']
    assert backend.catalog_calls==[] and first['games']['0']['publication']['final_version']==1
    assert not first['games']['1'].get('publication') and not first['games']['2'].get('publication')
    persisted=json.loads((tmp_path/'state.json').read_text())
    assert persisted['publication_rate_limit']['retry_after']=='3600'
    clock[0]+=timedelta(minutes=30)
    await scheduler.scan(config,backend,API(),TrackingTimers())
    assert len(backend.inspects)==2 and backend.catalog_calls==[]
    clock[0]+=timedelta(minutes=30);backend.throttle=False
    recovered=await scheduler.scan(config,backend,API(),TrackingTimers())
    assert backend.inspects==['qa/nba-published-0','qa/nba-published-1','qa/nba-published-1','qa/nba-published-2']
    assert len(backend.catalog_calls)==1 and len(backend.catalog_calls[0])==3
    assert 'publication_rate_limit' not in recovered

async def test_verified_publication_is_durable_before_catalog_failure(tmp_path):
    import pytest
    from nba_orderbook.scheduler import scan
    from nba_orderbook.model import now,iso
    g=game(iso(now()+timedelta(hours=3)));publication_state(tmp_path,[g])
    class API:
        async def discover(self):return [],[]
    class Backend(PublicationBackend):
        def catalog(self,*args):raise RuntimeError('catalog transport failed')
    backend=Backend([g]);config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    with pytest.raises(RuntimeError):await scan(config,backend,API(),TrackingTimers())
    assert json.loads((tmp_path/'state.json').read_text())['games'][g.game_id]['publication']['final_version']==1
    recovered=PublicationBackend([g])
    await scan(config,recovered,API(),TrackingTimers())
    assert recovered.inspects==[] and recovered.metadata_reads==[]

async def test_catalog_429_preserves_verified_cache_and_honors_http_date_retry_after(monkeypatch,tmp_path):
    import requests
    from nba_orderbook import scheduler
    from nba_orderbook.model import iso
    clock=[dt('2026-11-17T11:00:00Z')];monkeypatch.setattr(scheduler,'now',lambda:clock[0])
    g=game(iso(clock[0]+timedelta(hours=3)));publication_state(tmp_path,[g])
    class API:
        async def discover(self):return [],[]
    class Backend(PublicationBackend):
        throttle=True
        def catalog(self,rows,directory):
            self.catalog_calls.append(rows)
            if self.throttle:
                response=requests.Response();response.status_code=429
                response.headers['Retry-After']='Tue, 17 Nov 2026 12:00:00 GMT'
                raise requests.HTTPError('catalog rate limited',response=response)
    backend=Backend([g]);config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    first=await scheduler.scan(config,backend,API(),TrackingTimers())
    assert first['games'][g.game_id]['publication']['final_version']==1
    assert dt(first['publication_rate_limit']['retry_at'])==dt('2026-11-17T12:00:00Z')
    clock[0]+=timedelta(minutes=30)
    await scheduler.scan(config,backend,API(),TrackingTimers())
    assert len(backend.inspects)==1 and len(backend.catalog_calls)==1
    clock[0]+=timedelta(minutes=30);backend.throttle=False
    recovered=await scheduler.scan(config,backend,API(),TrackingTimers())
    assert len(backend.inspects)==1 and len(backend.catalog_calls)==2
    assert 'publication_rate_limit' not in recovered

async def test_real_backend_pending_catalog_429_stops_immediately_and_recovers_after_wait(monkeypatch,tmp_path):
    import requests
    from types import SimpleNamespace as N
    from unittest.mock import Mock
    from nba_orderbook import scheduler
    from nba_orderbook.kaggle import Backend
    from nba_orderbook.model import iso
    clock=[dt('2026-11-17T11:00:00Z')];monkeypatch.setattr(scheduler,'now',lambda:clock[0])
    g=game(iso(clock[0]-timedelta(hours=12)))
    row={'slug':g.slug,'game_start':g.start,'dataset':'qa/'+g.slug,'final_version':1,'stop_reason':'market_resolved','quality':'partial'}
    scheduler.Store(tmp_path).save({'games':{'1':{'game':g.to_dict(),'publication':row}},'batches':{},'anomalies':[]})
    directory=tmp_path/'catalog';directory.mkdir()
    ref='qa/nba-polymarket-orderbook';marker=directory/'.publish-state.json'
    marker.write_text(json.dumps({'ref':ref,'version':2,'status':'pending'}))
    response=requests.Response();response.status_code=429;response.headers['Retry-After']='3600'
    error=requests.HTTPError('rate limit in real Backend.catalog pending wait',response=response)
    ready={'ref':ref,'version':2,'status':'READY','files':['catalog.json']}
    api=Mock();backend=Backend('qa',client=N(datasets=N(dataset_api_client=api)),ready_timeout=30,poll_seconds=10)
    backend.list_active=Mock(return_value=[])
    backend.inspect_dataset=Mock(side_effect=[error,ready,ready])
    backend.read_json=Mock(return_value=[row]);backend._upload=Mock()
    sleep=Mock();monkeypatch.setattr('nba_orderbook.kaggle.time.sleep',sleep)
    class API:
        async def discover(self):return [],[]
    config={'state_dir':str(tmp_path),'owner':'qa','_path':'unused'}
    first=await scheduler.scan(config,backend,API(),TrackingTimers())
    assert backend.inspect_dataset.call_count==1 and sleep.call_count==0
    assert first['publication_rate_limit']['retry_after']=='3600'
    assert first['games']['1']['publication']==row
    assert json.loads(marker.read_text())=={'ref':ref,'version':2,'status':'pending'}
    assert json.loads((tmp_path/'state.json').read_text())['publication_rate_limit']['retry_after']=='3600'
    clock[0]+=timedelta(minutes=30)
    await scheduler.scan(config,backend,API(),TrackingTimers())
    assert backend.inspect_dataset.call_count==1
    clock[0]+=timedelta(minutes=30)
    recovered=await scheduler.scan(config,backend,API(),TrackingTimers())
    assert backend.inspect_dataset.call_count==3 and sleep.call_count==0
    assert 'publication_rate_limit' not in recovered
    assert json.loads(marker.read_text())['status']=='ready'
    api.create_dataset.assert_not_called();api.create_dataset_version.assert_not_called();backend._upload.assert_not_called()

async def test_collector_waits_for_429_without_blocking_other_game_and_reconciles_pending(monkeypatch,tmp_path):
    import requests,threading
    from types import SimpleNamespace as N
    from unittest.mock import Mock
    from nba_orderbook import collector
    from nba_orderbook.kaggle import Backend,FINAL_FILES
    from kagglesdk.datasets.types.dataset_api_service import ApiDatasetNewFile
    from nba_orderbook.model import now,iso
    original_sleep=asyncio.sleep
    clock=[now()];monkeypatch.setattr(collector,'now',lambda:clock[0])
    monkeypatch.setenv('KAGGLE_KERNEL_RUN_TYPE','Batch')
    rejected=threading.Event();progress=[];waits=[]
    a=game(iso(clock[0]+timedelta(hours=2)),'A');a.slug='nba-a'
    b=game(a.start,'B');b.slug='nba-b'
    async def run(self):
        self.directory.mkdir(parents=True,exist_ok=True)
        with gzip.open(self.directory/'orderbook.jsonl.gz','wt') as stream:stream.write('{}\n')
        (self.directory/'metadata.json').write_text(json.dumps(self.game.to_dict()))
        (self.directory/'summary.json').write_text(json.dumps({'stop_reason':'qa_timeout'}))
        if self.game.game_id=='B':
            while not rejected.is_set():await original_sleep(.001)
            progress.append('B continued while A waited')
        return {'stop_reason':'qa_timeout'}
    monkeypatch.setattr(collector.Collection,'run',run)
    async def sports(*args):await asyncio.Event().wait()
    monkeypatch.setattr(collector,'sports_updates',sports)
    async def wait(delay):
        waits.append(delay)
        while not progress:await original_sleep(.001)
        clock[0]+=timedelta(seconds=delay)
    monkeypatch.setattr(collector.asyncio,'sleep',wait)
    response=requests.Response();response.status_code=429;response.headers['Retry-After']='5'
    error=requests.HTTPError('create result unknown after 429',response=response)
    api=Mock()
    def create(request):
        assert api.create_dataset.call_count==1,'pending creation must not be repeated'
        rejected.set();raise error
    api.create_dataset.side_effect=create
    backend=Backend('qa',client=N(datasets=N(dataset_api_client=api)))
    backend._upload=Mock(return_value=ApiDatasetNewFile())
    backend.inspect_dataset=Mock(side_effect=lambda ref:None if api.create_dataset.call_count==0 else {'ref':ref,'version':1,'status':'READY','files':list(FINAL_FILES)})
    publish=backend.publish
    def publish_game(directory,ref,**kwargs):
        return {'status':'READY','version':1} if ref.endswith('nba-b') else publish(directory,ref,**kwargs)
    backend.publish=publish_game
    class API:
        async def close(self):pass
    results=await collector.collect_batch({'games':[a.to_dict(),b.to_dict()],'owner':'qa','qa_seconds':1,'runtime_seconds':100},tmp_path,backend,API())
    assert waits==[5] and progress==['B continued while A waited']
    assert len(results)==2 and api.create_dataset.call_count==1
    assert backend._upload.call_count==3
    assert json.loads((tmp_path/a.slug/'.publish-state.json').read_text())['status']=='ready'

async def test_collector_rate_limit_wait_never_extends_instance_budget(monkeypatch,tmp_path):
    import requests,pytest
    from nba_orderbook import collector
    from nba_orderbook.model import now,iso
    monkeypatch.setenv('KAGGLE_KERNEL_RUN_TYPE','Batch')
    original_sleep=asyncio.sleep
    for after,expire_while_waiting in (('600',False),('5',True)):
        clock=[now()];monkeypatch.setattr(collector,'now',lambda:clock[0]);waits=[]
        g=game(iso(clock[0]+timedelta(hours=2)))
        async def run(self):return {'stop_reason':'qa_timeout'}
        monkeypatch.setattr(collector.Collection,'run',run)
        async def sports(*args):await asyncio.Event().wait()
        monkeypatch.setattr(collector,'sports_updates',sports)
        async def wait(delay):waits.append(delay);clock[0]+=timedelta(seconds=11)
        monkeypatch.setattr(collector.asyncio,'sleep',wait)
        class Backend:
            calls=0
            def publish(self,*args,**kwargs):
                self.calls+=1;response=requests.Response();response.status_code=429;response.headers['Retry-After']=after
                raise requests.HTTPError('final publication throttled',response=response)
        class API:
            async def close(self):pass
        directory=tmp_path/after;directory.mkdir();backend=Backend()
        with pytest.raises(RuntimeError,match='batch_has_failed_games_or_publications'):
            await collector.collect_batch({'games':[g.to_dict()],'owner':'qa','qa_seconds':1,'runtime_seconds':10},directory,backend,API())
        assert backend.calls==1 and waits==([5] if expire_while_waiting else [])
        assert json.loads((directory/'batch-results.json').read_text())==[{'error':'HTTPError'}]
