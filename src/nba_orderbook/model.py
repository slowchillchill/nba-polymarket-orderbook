"""比赛身份与 UTC 时间规则。"""
from __future__ import annotations
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

UTC = timezone.utc

def now():
    return datetime.now(UTC)

def dt(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(UTC)

def iso(value):
    return dt(value).isoformat()

@dataclass
class Game:
    game_id: str
    slug: str
    event_id: str
    market_id: str
    condition_id: str
    assets: list[str]
    outcomes: list[str]
    start: str
    version: str = 'v1'
    status: str = ''
    started: bool = False
    time_changes: list[dict] = field(default_factory=list)
    teams: list = field(default_factory=list)
    postponed_with_time: bool = False

    @property
    def window(self):
        return dt(self.start) - timedelta(minutes=90)

    @property
    def deadline(self):
        return dt(self.start) + timedelta(hours=6)

    def reschedule(self, start, status='', live=False):
        changed=bool(start and not self.started and dt(start)!=dt(self.start))
        if status.lower()!='postponed':
            self.postponed_with_time=False
        elif changed:
            self.postponed_with_time=True
        elif self.status.lower()!='postponed':
            self.postponed_with_time=False
        if changed:
            self.time_changes.append({'observed_at': iso(now()), 'old': self.start, 'new': iso(start)})
            self.start = iso(start)
        self.status = status
        self.started = self.started or live

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

def group_games(games):
    groups = []
    for game in sorted(games, key=lambda g: dt(g.start)):
        if not groups or dt(game.start) - dt(groups[-1][0].start) > timedelta(hours=2):
            groups.append([])
        groups[-1].append(game)
    return groups

def peak_concurrency(groups):
    points = []
    for games in groups:
        points += [(min(g.window for g in games), 1), (max(g.deadline for g in games), -1)]
    active = peak = 0
    for _, delta in sorted(points):
        active += delta
        peak = max(peak, active)
    return peak
