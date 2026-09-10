"""Durable Postgres checkpoint storage; caller supplies an authorized connection."""
import json

class PostgresCheckpointStore:
    def __init__(self, connection):
        if not connection.autocommit:
            raise ValueError('Checkpoint connection must use autocommit for durable claims')
        self.connection=connection
    def claim(self,c,now):
        row=self.connection.execute('''INSERT INTO public.nfl_edge_checkpoints
            (checkpoint_key,game_id,kickoff,decision_window,target_at,deadline_at,started_at,status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,'running')
            ON CONFLICT (checkpoint_key) DO NOTHING RETURNING checkpoint_key''',
            (c.key,c.game_id,c.kickoff,c.name,c.target,c.deadline,now)).fetchone()
        return row is not None
    def finish(self,c,now,status,quotes,error):
        if status not in {'captured','missed','unavailable','failed'}: raise ValueError('Invalid receipt state')
        if status!='captured' and quotes: raise ValueError('Only successful captures can retain quotes')
        row=self.connection.execute('''UPDATE public.nfl_edge_checkpoints
            SET ended_at=%s,status=%s,quotes=%s::jsonb,error=%s
            WHERE checkpoint_key=%s AND status='running' RETURNING checkpoint_key''',
            (now,status,json.dumps(quotes,allow_nan=False),error,c.key)).fetchone()
        if row is None: raise RuntimeError('Checkpoint was not claimed or is already final')
    def reconcile(self,now):
        # A claim with no completion is never automatically retried against a
        # paid provider: the process might have crashed after paying for it.
        return self.connection.execute('''UPDATE public.nfl_edge_checkpoints
            SET status='abandoned',ended_at=%s,error='Claim expired without completion; no automatic paid retry'
            WHERE status='running' AND deadline_at<=%s''',(now,now)).rowcount
