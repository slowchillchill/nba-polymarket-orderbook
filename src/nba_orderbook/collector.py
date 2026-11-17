"""每场独立接收、停止和最终发布。实际采集入口仅允许 Kaggle。"""
from __future__ import annotations
import asyncio
import gzip
import json
import os
from pathlib import Path
import uuid
from datetime import timedelta
import websockets
from .book import Book
from .model import Game, dt, iso, now
from .polymarket import Polymarket

MARKET_WS = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
SPORTS_WS = 'wss://sports-api.polymarket.com/ws'
HEARTBEAT_INTERVAL = 10
PONG_TIMEOUT = 20

def write_json(path, data):
    path.write_text(json.dumps(data,ensure_ascii=False,indent=2,default=str)+'\n')

class Collection:
    def __init__(self, game, directory, api, budget_end, qa_seconds=None, connector=websockets.connect):
        self.game, self.directory, self.api = game, directory, api
        self.budget_end, self.qa_seconds, self.connector = budget_end, qa_seconds, connector
        self.book = Book(game.assets)
        self.stop = asyncio.Event()
        self.reason = None
        self.resolution = None
        self.gaps = []
        self.first_snapshots = {}
        self.messages = 0
        self.runtime_started_at = now()
        self.started_at = None
        self.next_schedule_check = dt(game.start)-timedelta(minutes=10)
        self.connection_id = None
        self.socket = None
        self.disconnect_at = None
        self.paused = False

    async def controls(self):
        next_resolution = now()
        while not self.stop.is_set():
            at = now()
            if at >= self.game.deadline:
                self.reason = 'six_hour_deadline'
            elif at >= self.budget_end:
                self.reason = 'runtime_budget_interruption'
            elif self.qa_seconds and at >= self.runtime_started_at+timedelta(seconds=self.qa_seconds):
                self.reason = 'qa_timeout'
            if self.reason:
                self.stop.set()
                return
            if at >= next_resolution:
                try:
                    self.resolution = await self.api.resolution(self.game)
                    if self.resolution:
                        self.reason = 'market_resolved'
                        self.stop.set()
                        return
                except Exception as e:
                    self.gaps.append({'kind':'resolution_query_failed','at':iso(at),'error':type(e).__name__})
                next_resolution = at+timedelta(minutes=5)
            if not self.game.started and at >= self.next_schedule_check:
                self.next_schedule_check=at+timedelta(minutes=5)
                try:
                    await self.api.refresh(self.game)
                    new_t10=dt(self.game.start)-timedelta(minutes=10)
                    if at < new_t10 < self.next_schedule_check:
                        self.next_schedule_check=new_t10
                    status = self.game.status.lower()
                    if status == 'postponed' and not self.game.postponed_with_time:
                        self.reason = 'postponed_waiting_schedule'
                        self.stop.set()
                        return
                    if self.game.deadline > self.budget_end:
                        self.reason = 'rescheduled_beyond_budget'
                        self.stop.set()
                        return
                    if self.socket and at < self.game.window:
                        await self.socket.close()
                except Exception as e:
                    self.gaps.append({'kind':'schedule_check_failed','at':iso(at),'error':type(e).__name__})
            await asyncio.sleep(1)

    async def heartbeat(self, socket, pong):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            pong.clear()
            sent_at=now()
            try:
                await socket.send('PING')
                await asyncio.wait_for(pong.wait(),timeout=PONG_TIMEOUT)
            except Exception as error:
                self.gaps.append({'kind':'heartbeat_timeout' if isinstance(error,TimeoutError) else 'heartbeat_error',
                                  'start':iso(sent_at),'end':iso(now()),'error':type(error).__name__})
                await socket.close()
                return

    async def receive(self, stream):
        disconnect_at = None
        while not self.stop.is_set():
            if not self.qa_seconds and now() < self.game.window:
                await asyncio.sleep(min(1,(self.game.window-now()).total_seconds()))
                continue
            try:
                self.connection_id = uuid.uuid4().hex
                self.book.snapshots.clear()
                async with self.connector(MARKET_WS, max_size=None, ping_interval=None, close_timeout=5) as socket:
                    self.socket = socket
                    await socket.send(json.dumps({'assets_ids':self.game.assets,'type':'market','custom_feature_enabled':True}))
                    pong=asyncio.Event()
                    heartbeat = asyncio.create_task(self.heartbeat(socket,pong))
                    try:
                        async for raw in socket:
                            received = now()
                            if self.started_at is None:
                                self.started_at=received
                            if isinstance(raw,bytes):
                                raw = raw.decode('utf-8',errors='surrogateescape')
                            self.messages += 1
                            stream.write(json.dumps({'received_at':iso(received),'connection_id':self.connection_id,
                                                     'sequence':self.messages,'raw':raw},ensure_ascii=True)+'\n')
                            if raw == 'PONG':
                                pong.set()
                            try:
                                events = json.loads(raw)
                                for event in events if isinstance(events,list) else [events]:
                                    if not isinstance(event,dict):
                                        continue
                                    self.book.apply(event)
                                    if event.get('event_type') == 'book' and event.get('asset_id') in self.game.assets:
                                        self.first_snapshots.setdefault(event['asset_id'],iso(received))
                                    if len(self.book.snapshots)==2 and disconnect_at:
                                        self.gaps.append({'kind':'disconnect','start':disconnect_at,'end':iso(received),
                                                          'historical_deltas_recoverable':False})
                                        disconnect_at = None
                                        self.disconnect_at = None
                                    if event.get('event_type') == 'market_resolved' and event.get('market') == self.game.condition_id:
                                        self.resolution = event
                                        self.reason = 'market_resolved'
                                        self.stop.set()
                                        return
                            except (ValueError,TypeError,KeyError):
                                pass
                            if self.stop.is_set():
                                return
                    finally:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat,return_exceptions=True)
                        self.socket = None
            except Exception as e:
                if not disconnect_at:
                    disconnect_at = iso(now())
                    self.disconnect_at = disconnect_at
                self.gaps.append({'kind':'connection_error','at':iso(now()),'error':type(e).__name__})
                await asyncio.sleep(1)
            if not disconnect_at:
                disconnect_at = iso(now())
                self.disconnect_at = disconnect_at
        if disconnect_at:
            self.gaps.append({'kind':'disconnect','start':disconnect_at,'end':iso(now()),'historical_deltas_recoverable':False})

    async def run(self):
        self.directory.mkdir(parents=True,exist_ok=True)
        control = asyncio.create_task(self.controls())
        with gzip.open(self.directory/'orderbook.jsonl.gz','wt',encoding='utf-8') as stream:
            receive = asyncio.create_task(self.receive(stream))
            try:
                await self.stop.wait()
            finally:
                receive.cancel()
                control.cancel()
                await asyncio.gather(receive,control,return_exceptions=True)
        ended = now()
        if self.started_at is None:
            self.gaps.append({'kind':'no_messages','target':iso(self.game.window),'ended_at':iso(ended)})
        elif self.started_at > self.game.window:
            self.gaps.append({'kind':'late_start','target':iso(self.game.window),'actual':iso(self.started_at)})
        if self.disconnect_at:
            self.gaps.append({'kind':'disconnect','start':self.disconnect_at,'end':iso(ended),'historical_deltas_recoverable':False})
        on_time = len(self.first_snapshots)==2 and all(dt(t)<=dt(self.game.start)-timedelta(minutes=1) for t in self.first_snapshots.values())
        summary = {'slug':self.game.slug,'condition_id':self.game.condition_id,'started_at':iso(self.started_at) if self.started_at else None,
                   'runtime_started_at':iso(self.runtime_started_at),'game_start':self.game.start,
                   'coverage_started_at':max(self.first_snapshots.values(),key=dt) if len(self.first_snapshots)==2 else None,
                   'ended_at':iso(ended),'stop_reason':self.reason or 'interrupted','resolution':self.resolution,
                   'message_count':self.messages,'first_snapshots':self.first_snapshots,'on_time_snapshots':on_time,
                   'gaps':self.gaps,'quality':'complete' if on_time and not self.gaps and self.reason in ('market_resolved','six_hour_deadline') else 'partial'}
        write_json(self.directory/'metadata.json',self.game.to_dict())
        write_json(self.directory/'summary.json',summary)
        return summary

async def sports_updates(games):
    while True:
        try:
            async with websockets.connect(SPORTS_WS,ping_interval=None) as socket:
                async for raw in socket:
                    if raw == 'ping':
                        await socket.send('pong')
                        continue
                    try:
                        event=json.loads(raw)
                        for game in games:
                            if event.get('slug') == game.slug and event.get('live'):
                                game.started=True
                    except (ValueError,AttributeError):
                        pass
        except Exception:
            await asyncio.sleep(5)

async def collect_batch(batch, output, backend, api=None):
    if not os.environ.get('KAGGLE_KERNEL_RUN_TYPE'):
        raise RuntimeError('collect_requires_kaggle')
    games=[Game.from_dict(x) for x in batch['games']]
    api=api or Polymarket()
    started=now()
    budget_end=started+timedelta(seconds=batch.get('runtime_seconds',39600))
    sports=asyncio.create_task(sports_updates(games))
    async def one(game):
        directory=Path(output)/game.slug
        summary=await Collection(game,directory,api,budget_end,batch.get('qa_durations',{}).get(game.game_id,batch.get('qa_seconds'))).run()
        ref=f"{batch['owner']}/"+(('nba-qa-'+game.slug) if batch.get('qa_seconds') else game.slug)
        if summary['stop_reason'] in ('postponed_waiting_schedule','rescheduled_beyond_budget'):
            return summary
        print(json.dumps({'event':'publication_started','slug':game.slug,'at':iso(now())}),flush=True)
        published=await asyncio.to_thread(backend.publish,directory,ref,public=not bool(batch.get('qa_seconds')))
        print(json.dumps({'event':'publication_ready','slug':game.slug,'at':iso(now()),'version':published.get('version')}),flush=True)
        return {**summary,'dataset':published,'game_start':game.start}
    try:
        results=await asyncio.gather(*(one(g) for g in games),return_exceptions=True)
        write_json(Path(output)/'batch-results.json',[{'error':type(x).__name__} if isinstance(x,Exception) else x for x in results])
        if any(isinstance(x,Exception) for x in results):
            raise RuntimeError('batch_has_failed_games_or_publications')
        return results
    finally:
        sports.cancel()
        await asyncio.gather(sports,return_exceptions=True)
        await api.close()
