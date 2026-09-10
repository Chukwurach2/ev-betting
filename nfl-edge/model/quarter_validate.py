"""Direct Q1-total experiment; never promotes models or publishes picks."""
import argparse
import datetime as dt
import hashlib
import json
import pathlib
import sys
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
sys.path.insert(0, str(pathlib.Path(__file__).parent / 'src'))
from nfl_early.data import download_season

LINES = [3.5, 6.5, 7.5, 9.5, 10.5, 13.5]
FEATURES = ['home_for', 'home_against', 'away_for', 'away_against']
ALPHA = .2

def quarter_scores(pbp):
    required = ['game_id','season','week','home_team','away_team','qtr','quarter_end',
                'total_home_score','total_away_score','play_id']
    if set(required) - set(pbp.columns): raise ValueError('Missing quarter scoreboard columns')
    x = pbp[pbp.season_type.eq('REG')].copy() if 'season_type' in pbp else pbp.copy()
    # Scores describe START of play. Require the explicit Q1-end marker,
    # which follows Q1 scoring and excludes points from Q2.
    end = x[x.qtr.eq(1) & x.quarter_end.eq(1)].sort_values('play_id')
    if end.game_id.duplicated().any(): raise ValueError('Ambiguous duplicate Q1-end markers')
    if set(x.game_id.dropna()) != set(end.game_id.dropna()):
        raise ValueError('Missing Q1-end marker; no synthetic labels allowed')
    cols = ['game_id','season','week','home_team','away_team','total_home_score','total_away_score']
    end = end[cols].rename(columns={'total_home_score':'home_points','total_away_score':'away_points'})
    for c in ['home_points','away_points']:
        end[c] = pd.to_numeric(end[c],errors='raise')
        if not np.isfinite(end[c]).all() or not ((end[c]>=0)&(end[c]%1==0)).all():
            raise ValueError('Invalid quarter scoreboard')
    end['total'] = end.home_points + end.away_points
    return end.sort_values(['season','week','game_id']).reset_index(drop=True)

def pregame_features(games):
    states, rows = {}, []
    for _,week in games.sort_values(['season','week','game_id']).groupby(['season','week'],sort=True):
        # Freeze a whole week: same-week outcomes never enter pregame features.
        for r in week.to_dict('records'):
            h = states.get(r['home_team'],[4.5,4.5,0]); a = states.get(r['away_team'],[4.5,4.5,0])
            rows.append({**r,**dict(zip(FEATURES,[h[0],h[1],a[0],a[1]])), 'prior_games':min(h[2],a[2])})
        for r in week.to_dict('records'):
            for team,scored,allowed in [(r['home_team'],r['home_points'],r['away_points']),
                                        (r['away_team'],r['away_points'],r['home_points'])]:
                old = states.get(team,[4.5,4.5,0])
                states[team] = [(1-ALPHA)*old[0]+ALPHA*scored,(1-ALPHA)*old[1]+ALPHA*allowed,old[2]+1]
    return pd.DataFrame(rows), states

def predict_portable(artifact,features):
    x = np.asarray(features,dtype=float)
    if x.ndim!=2 or x.shape[1]!=len(FEATURES) or not np.isfinite(x).all():
        raise ValueError('Invalid quarter-model features')
    z = (x-np.asarray(artifact['mean']))/np.asarray(artifact['scale'])
    logits = z@np.asarray(artifact['coefficients']).T+np.asarray(artifact['intercepts'])
    p = 1/(1+np.exp(-np.clip(logits,-35,35)))
    return np.minimum.accumulate(p,axis=1)

def losses(y,p):
    p=np.clip(p,1e-9,1-1e-9)
    return -(y*np.log(p)+(1-y)*np.log(1-p))

def block_interval(delta,seasons,weeks,samples=2000):
    frame=pd.DataFrame({'d':delta,'season':seasons,'week':weeks})
    blocks=[g.d.to_numpy() for _,g in frame.groupby(['season','week'])]
    rng=np.random.default_rng(20260910)
    means=[float(np.concatenate([blocks[i] for i in rng.integers(0,len(blocks),len(blocks))]).mean()) for _ in range(samples)]
    return np.quantile(means,[.025,.975]).tolist()

def train_validate(games):
    f,states=pregame_features(games)
    prefix,_=pregame_features(games[games.season<=2022])
    pd.testing.assert_frame_equal(f[f.season<=2022].reset_index(drop=True),prefix.reset_index(drop=True))
    train=f[(f.season<=2022)&(f.prior_games>=3)]; test=f[(f.season>=2023)&(f.prior_games>=3)]
    if len(train)<1000 or len(test)<500: raise ValueError('Insufficient chronological validation games')
    scaler=StandardScaler().fit(train[FEATURES]); coefficients=[]; intercepts=[]; baseline=[]
    for line in LINES:
        y=train.total.gt(line).astype(int)
        model=LogisticRegression(C=.5,max_iter=2000,class_weight=None).fit(scaler.transform(train[FEATURES]),y)
        coefficients.append(model.coef_[0].tolist()); intercepts.append(float(model.intercept_[0]))
        baseline.append(float((y.sum()+1)/(len(y)+2)))
    artifact={'version_name':'nfl-edge-q1-direct-shadow-v1','role':'challenger','production_validated':False,
              'market':'Q1_TOTAL','lines':LINES,'features':FEATURES,'mean':scaler.mean_.tolist(),
              'scale':scaler.scale_.tolist(),'coefficients':coefficients,'intercepts':intercepts,
              'states':states,'state_cutoff_season':int(games.season.max()),'alpha':ALPHA,
              'cold_start':[4.5,4.5,0],'minimum_prior_games':3}
    probability=predict_portable(artifact,test[FEATURES])
    actual=(test.total.to_numpy()[:,None]>np.asarray(LINES)).astype(float)
    prior=np.tile(baseline,(len(test),1))
    ml,bl=losses(actual,probability),losses(actual,prior)
    interval=block_interval((ml-bl).mean(axis=1),test.season.to_numpy(),test.week.to_numpy())
    brier=float(np.mean((probability-actual)**2)); base_brier=float(np.mean((prior-actual)**2))
    report={'version_name':artifact['version_name'],'production_validated':False,
            'training_seasons':sorted(train.season.unique().astype(int).tolist()),
            'validation_seasons':sorted(test.season.unique().astype(int).tolist()),
            'train_games':len(train),'validation_games':len(test),
            'log_loss':float(ml.mean()),'baseline_log_loss':float(bl.mean()),
            'brier':brier,'baseline_brier':base_brier,'log_loss_delta_week_block_95ci':interval,
            'prediction_accuracy_gate_passed':bool(interval[1]<0 and brier<base_brier),
            'by_line':[{'line':line,'games':len(test),'log_loss':float(ml[:,i].mean()),
                        'baseline_log_loss':float(bl[:,i].mean()),
                        'brier':float(np.mean((probability[:,i]-actual[:,i])**2)),
                        'baseline_brier':float(np.mean((prior[:,i]-actual[:,i])**2))} for i,line in enumerate(LINES)],
            'feature_availability_audit':True,'label_method':'Q1-end scoreboard marker; scores at start of play',
            'feature_method':'Historical Q1 points only; freeze before each season/week',
            'limitations':['Fixed half-point grid, not historical executable quotes','No historical EV, ROI, or CLV validation',
                           'No verified pregame injuries, starters or weather inputs','No automatic model promotion'],
            'created_at':dt.datetime.now(dt.timezone.utc).isoformat()}
    predictions=test[['game_id','season','week','total']].copy()
    for i,line in enumerate(LINES): predictions[f'over_{line}']=probability[:,i]
    return artifact,report,predictions

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--data-dir',required=True)
    parser.add_argument('--out',required=True); parser.add_argument('--download',action='store_true')
    args=parser.parse_args(); frames=[]; hashes={}
    for season in range(2016,2026):
        path=pathlib.Path(args.data_dir)/f'play_by_play_{season}.parquet'
        if args.download: download_season(season,args.data_dir)
        cols=['game_id','season','season_type','week','home_team','away_team','qtr','quarter_end','total_home_score','total_away_score','play_id']
        frames.append(quarter_scores(pd.read_parquet(path,columns=cols)))
        hashes[str(season)]=hashlib.sha256(path.read_bytes()).hexdigest()
        print('Loaded Q1 labels',season,len(frames[-1]),flush=True)
    artifact,report,predictions=train_validate(pd.concat(frames,ignore_index=True))
    out=pathlib.Path(args.out); out.mkdir(parents=True,exist_ok=True); model_path=out/'quarter-model.json'
    model_path.write_text(json.dumps(artifact,indent=2,allow_nan=False))
    report['artifact_sha256']=hashlib.sha256(model_path.read_bytes()).hexdigest(); report['data_sha256']=hashes
    (out/'quarter-validation.json').write_text(json.dumps(report,indent=2,allow_nan=False))
    predictions.to_csv(out/'heldout-quarter-predictions.csv',index=False)
    print(json.dumps(report,indent=2,allow_nan=False),flush=True)

if __name__=='__main__': main()
