from __future__ import annotations
import os,json,datetime as dt,sys,pathlib
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]/'model'))
from production_gate import validate_artifact
from provider_oddsapi import fetch_events, fetch_event_markets, fetch_event_odds, available_supported_market_keys, normalize, quota

def run(model_dir):
 r={'checked_at':dt.datetime.now(dt.timezone.utc).isoformat()};r['model']=validate_artifact(model_dir)
 try:
  events,h=fetch_events(); now=dt.datetime.now(dt.timezone.utc)
  upcoming=[]
  for e in events:
   try:k=dt.datetime.fromisoformat(e['commence_time'].replace('Z','+00:00'))
   except Exception:continue
   mins=(k-now).total_seconds()/60
   if 0 < mins <= 30*60: upcoming.append((mins,e))
  upcoming.sort(key=lambda x:x[0])
  q=[]; inspected=[]; qstate=quota(h)
  # Readiness certification samples only the nearest upcoming event. Production scheduler should query only due checkpoints.
  if upcoming:
   _,e=upcoming[0]; mr,mh=fetch_event_markets(e['id']); keys=available_supported_market_keys(mr); inspected=keys; qstate=quota(mh)
   if keys:
    ev,oh=fetch_event_odds(e['id'],keys); q=normalize(ev); qstate=quota(oh)
  r['odds']={'ready':bool(q),'provider':'the_odds_api','normalized_quotes':len(q),'supported_markets_seen':inspected,'quota':qstate,'reason':None if q else ('provider reachable; no currently mapped supported NY quotes on sampled upcoming event' if upcoming else 'provider reachable; no NFL event inside next 30h')}
 except Exception as e:r['odds']={'ready':False,'provider':'the_odds_api','normalized_quotes':0,'reason':str(e)}
 r['ready']=bool(r['model'].get('ready') and r['odds'].get('ready'));print(json.dumps(r,indent=2));return 0 if r['ready'] else 2
if __name__=='__main__':raise SystemExit(run(sys.argv[1] if len(sys.argv)>1 else 'artifacts'))
