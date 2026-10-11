-- SHADOW operational metadata only; no quotes, prices, outcomes or private bets.
-- Read-only audit, not a scheduler, collector, retry mechanism or promotion gate.
-- Mirrors the current checkpoint timing contract: 90-minute windows capped at kickoff-1m.
-- Scope: today's scheduled NFL games in America/New_York; excludes opener/props.
-- Captured is the stored status, NOT certification of quote freshness or completeness.
-- Missing rows are classified independently; preserve elapsed gaps, never backfill.
-- Reviewed production mapping required before wiring into app health.
WITH expected AS (
  SELECT g.game_id, g.kickoff, w.window_name,
         g.kickoff - w.lead AS target_at,
         least(g.kickoff - w.lead + interval '90 minutes',
               g.kickoff - interval '1 minute') AS deadline_at
  FROM public.games g
  CROSS JOIN (VALUES ('T-24',interval '24 hours'),
    ('T-3',interval '3 hours'), ('T-90',interval '90 minutes'),
    ('Close',interval '5 minutes')) w(window_name,lead)
  WHERE g.sport='nfl' AND g.status='scheduled'
    AND (g.kickoff AT TIME ZONE 'America/New_York')::date =
        (now() AT TIME ZONE 'America/New_York')::date
), observations AS (
 SELECT e.*, count(c.checkpoint_key) AS records,
        min(c.status) AS stored_status,
        min(c.error) AS stored_error,
        min(c.started_at) AS first_started_at,
        max(c.ended_at) AS last_ended_at
 FROM expected e LEFT JOIN public.nfl_edge_checkpoints c
 ON c.game_id=e.game_id AND c.kickoff=e.kickoff AND c.decision_window=e.window_name
 GROUP BY e.game_id,e.kickoff,e.window_name,e.target_at,e.deadline_at
), classified AS (
 SELECT *, CASE WHEN records>1 THEN 'duplicate_records'
 WHEN records=1 AND stored_status='failed' THEN
  CASE WHEN stored_error='Provider event did not match exact matchup and kickoff'
         THEN 'failed_identity_mismatch'
       WHEN stored_error='Provider response event changed'
         THEN 'failed_provider_event_changed'
       WHEN stored_error='Odds provider request failed'
         OR stored_error LIKE 'Odds provider request failed with HTTP %'
         THEN 'failed_provider_request'
       WHEN stored_error='Collection failed; inspect private runtime health'
         THEN 'failed_private_runtime'
       ELSE 'failed_other_redacted' END
 WHEN records=1 THEN coalesce(stored_status,'invalid_status')
 WHEN now()<target_at THEN 'not_due'
 WHEN now()<deadline_at THEN 'due_missing_record'
 ELSE 'elapsed_missing_record' END AS capture_state
 FROM observations
)
SELECT now() AS checked_at, window_name, capture_state, count(*) AS games,
 min(target_at) AS earliest_target, min(deadline_at) AS earliest_deadline,
 min(first_started_at) AS earliest_started_at,
 max(last_ended_at) AS latest_ended_at
FROM classified GROUP BY window_name,capture_state
ORDER BY window_name,capture_state;
