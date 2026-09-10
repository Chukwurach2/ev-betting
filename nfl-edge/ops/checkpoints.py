"""Checkpoint planner and bounded collection worker. No wager publication."""
from __future__ import annotations
import datetime as dt
import hashlib
from dataclasses import dataclass

UTC=dt.timezone.utc
WINDOWS=(('T-24',1440),('T-3',180),('T-90',90),('Close',5))

def instant(value):
    t=value if isinstance(value,dt.datetime) else dt.datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if t.tzinfo is None: raise ValueError('Timestamp must include timezone')
    return t.astimezone(UTC)

@dataclass(frozen=True)
class Checkpoint:
    key:str
    game_id:str
    kickoff:dt.datetime
    name:str
    target:dt.datetime
    deadline:dt.datetime
    state:str

def plan(games,now):
    now=instant(now); result={}
    for game in games:
        if game.get('status','scheduled')!='scheduled': continue
        kickoff=instant(game['kickoff'])
        # No historical mass backfill. Each run observes a bounded time range.
        if not -1440 <= (kickoff-now).total_seconds()/60 <= 1440: continue
        for name,minutes in WINDOWS:
            target=kickoff-dt.timedelta(minutes=minutes)
            deadline=min(target+dt.timedelta(minutes=15),kickoff)
            if now<target: continue
            state='due' if now<deadline else 'missed'
            identity='|'.join([str(game['game_id']),kickoff.isoformat(),name])
            key=hashlib.sha256(identity.encode()).hexdigest()
            result[key]=Checkpoint(key,str(game['game_id']),kickoff,name,target,deadline,state)
    return sorted(result.values(),key=lambda c:(c.deadline,c.game_id,c.name))

def collect(checkpoints,store,fetch_quotes,clock,remaining,max_requests=2,reserve=10):
    """Store.claim must commit a UNIQUE key before any paid request.

    No automatic retry of claimed work: after a crash a separate reconciliation
    pass marks abandoned claims, avoiding duplicate provider charges.
    """
    if not isinstance(max_requests,int) or isinstance(max_requests,bool) or not 0<=max_requests<=16:
        raise ValueError('Invalid request cap')
    used=0; counts={'captured':0,'missed':0,'duplicate':0,'deferred':0,'failed':0,'requests':0}
    for checkpoint in checkpoints:
        now=instant(clock())
        missed=checkpoint.state=='missed' or now>=checkpoint.deadline
        if not missed and (used>=max_requests or not isinstance(remaining,int) or remaining-used<=reserve):
            counts['deferred']+=1; continue
        if not store.claim(checkpoint,now): counts['duplicate']+=1; continue
        if missed:
            store.finish(checkpoint,now,'missed',[],'Checkpoint elapsed; no historical quote invented')
            counts['missed']+=1; continue
        try:
            # Recheck AFTER the durable claim; DB delays can cross the deadline.
            if instant(clock())>=checkpoint.deadline:
                store.finish(checkpoint,instant(clock()),'missed',[],'Deadline elapsed before request')
                counts['missed']+=1; continue
            used+=1
            quotes=fetch_quotes(checkpoint)
            ended=instant(clock())
            if ended>=checkpoint.deadline:
                store.finish(checkpoint,ended,'missed',[],'Response arrived after checkpoint deadline')
                counts['missed']+=1; continue
            valid=[]
            for q in quotes:
                observed=instant(q['observed_at'])
                if checkpoint.target<=observed<=ended and (ended-observed).total_seconds()<=900:
                    valid.append(q)
            store.finish(checkpoint,ended,'captured' if valid else 'unavailable',valid,None)
            counts['captured' if valid else 'failed']+=1
        except Exception:
            # Neither provider URLs nor database connection strings reach receipts.
            store.finish(checkpoint,instant(clock()),'failed',[],'Collection failed; inspect private runtime health')
            counts['failed']+=1
    counts['requests']=used
    return counts
