"""公开发现及结算接口，不包含交易接口。"""
import json
from datetime import timedelta
import httpx
from .model import Game, dt, iso, now

GAMMA = 'https://gamma-api.polymarket.com'
DATA = 'https://data-api.polymarket.com'

def array(value):
    return json.loads(value) if isinstance(value, str) else value or []

class Polymarket:
    def __init__(self, client=None):
        self.client = client or httpx.AsyncClient(timeout=30)

    async def close(self):
        await self.client.aclose()

    async def get(self, path, params=None, base=GAMMA):
        r = await self.client.get(base + path, params=params)
        r.raise_for_status()
        return r.json()

    async def discover(self, at=None):
        at = at or now()
        sports = await self.get('/sports')
        nba = next(s for s in sports if s['sport'].lower() == 'nba')
        params = [('series_id', nba['series']), ('start_time_min', iso(at-timedelta(hours=6))),
                  ('start_time_max', iso(at+timedelta(days=7))), ('version','v1'), ('version','v2'), ('limit','100')]
        events = []
        cursor = None
        while True:
            page = await self.get('/events/keyset', params + ([('after_cursor',cursor)] if cursor else []))
            events.extend(page['events'])
            cursor = page.get('next_cursor')
            if not cursor:
                break
        return select_games(events)

    async def refresh(self, game):
        event = await self.get('/events/' + game.event_id)
        market = next((m for m in event.get('markets',[]) if m.get('conditionId') == game.condition_id), {})
        start = market.get('gameStartTime') or event.get('startTime')
        status = event.get('gameStatus') or event.get('status') or ''
        game.reschedule(start, status, bool(event.get('live')))
        return game

    async def resolution(self, game):
        result = await self.get('/v2/resolutions', {'condition':game.condition_id}, DATA)
        for row in result.get('data',[]):
            if row.get('condition_id') == game.condition_id and row.get('status') == 'resolved':
                return row
        return None

def select_games(events):
    grouped = {}
    anomalies = []
    for event in events:
        for market in event.get('markets',[]):
            if market.get('sportsMarketType') != 'moneyline':
                continue
            gid = str(market.get('gameId') or event.get('gameId') or '')
            if not gid:
                anomalies.append({'event_id':str(event['id']), 'reason':'missing_game_id'})
                continue
            grouped.setdefault(gid, {})[market['conditionId']] = (event,market)
    games = []
    for gid, candidates in grouped.items():
        if len(candidates) != 1:
            anomalies.append({'game_id':gid,'reason':'multiple_moneylines','conditions':list(candidates)})
            continue
        event,m = next(iter(candidates.values()))
        version = m.get('version') or 'v1'
        ids = array(m.get('positionIds') if version == 'v2' else m.get('clobTokenIds'))
        start = m.get('gameStartTime') or event.get('startTime')
        if len(ids) != 2 or not start:
            anomalies.append({'game_id':gid,'reason':'missing_assets_or_time'})
            continue
        games.append(Game(gid,event['slug'],str(event['id']),str(m['id']),m['conditionId'],ids,
                          array(m.get('outcomes')),iso(dt(start)),version,
                          event.get('gameStatus') or event.get('status') or '',bool(event.get('live')),teams=event.get('teams',[])))
    return games, anomalies
