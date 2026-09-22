-- SHADOW operational metadata only; returns aggregate readiness counts, never prices,
-- selections, outcomes, scores, pick identifiers, raw quotes or CLV values.
-- Read-only audit: not a collector, settlement job, retry, export or promotion gate.
--
-- A captured Close checkpoint with two exact-line books is only a coverage
-- candidate. It is NOT a verified closing price. Provider event IDs are
-- snapshot-scoped and are deliberately not used as cross-snapshot join keys.
-- Real CLV remains blocked until an independently retained frozen settled export
-- and owner-reviewed canonical-game / actual-close mapping exist.
WITH scoped AS (
  SELECT p.pick_id, p.mode, p.market, p.game_id, p.checkpoint_key,
         p.kickoff, p.observed_at, p.line, p.selection,
         p.result, p.settled_at, p.final_home_score, p.final_away_score,
         p.settled_by, p.taken_fair_prob, p.clv_prob_points,
         ec.game_id AS entry_game_id, ec.kickoff AS entry_kickoff,
         ec.status AS entry_status
  FROM public.nfl_edge_picks p
  LEFT JOIN public.nfl_edge_checkpoints ec
    ON ec.checkpoint_key = p.checkpoint_key
), close_coverage AS (
  SELECT p.pick_id,
         count(DISTINCT cc.checkpoint_key) AS close_records,
         bool_or(cc.status = 'captured') AS has_captured_close,
         count(q.quote_id) FILTER (
           WHERE cc.status = 'captured'
             AND q.market = p.market
             AND q.selection = p.selection
             AND q.line = p.line
             AND q.observed_at <= p.kickoff
         ) AS exact_line_candidate_quotes,
         count(DISTINCT q.book_key) FILTER (
           WHERE cc.status = 'captured'
             AND q.market = p.market
             AND q.selection = p.selection
             AND q.line = p.line
             AND q.observed_at <= p.kickoff
         ) AS exact_line_candidate_books
  FROM scoped p
  LEFT JOIN public.nfl_edge_checkpoints cc
    ON cc.game_id = p.game_id
   AND cc.kickoff = p.kickoff
   AND cc.decision_window = 'Close'
  LEFT JOIN public.nfl_edge_odds_quotes q
    ON q.checkpoint_key = cc.checkpoint_key
  GROUP BY p.pick_id
), classified AS (
  SELECT p.pick_id, p.mode, p.market,
    CASE
      WHEN p.result IS NULL AND (
        p.settled_at IS NOT NULL OR p.final_home_score IS NOT NULL
        OR p.final_away_score IS NOT NULL OR p.settled_by IS NOT NULL
        OR p.clv_prob_points IS NOT NULL
      ) THEN 'inconsistent_partial_settlement'
      WHEN p.result IS NULL THEN 'open_unsettled'
      WHEN p.settled_at IS NULL OR p.final_home_score IS NULL
        OR p.final_away_score IS NULL OR p.settled_by IS NULL
        THEN 'settled_metadata_incomplete'
      ELSE 'settled_metadata_complete'
    END AS settlement_state,
    CASE
      WHEN p.checkpoint_key IS NULL THEN 'missing_entry_checkpoint_key'
      WHEN p.entry_game_id IS NULL THEN 'missing_entry_checkpoint'
      WHEN p.entry_game_id IS DISTINCT FROM p.game_id
        OR p.entry_kickoff IS DISTINCT FROM p.kickoff
        THEN 'entry_checkpoint_identity_mismatch'
      WHEN p.entry_status IS DISTINCT FROM 'captured'
        THEN 'entry_checkpoint_not_captured'
      WHEN p.observed_at > p.kickoff THEN 'entry_after_kickoff'
      WHEN p.taken_fair_prob IS NULL THEN 'missing_taken_fair_probability'
      ELSE 'entry_metadata_ready'
    END AS entry_state,
    CASE
      WHEN c.close_records = 0 THEN 'missing_close_checkpoint'
      WHEN c.close_records > 1 THEN 'duplicate_close_checkpoints'
      WHEN NOT coalesce(c.has_captured_close, false)
        THEN 'close_checkpoint_not_captured'
      WHEN c.exact_line_candidate_quotes = 0
        THEN 'no_exact_line_close_candidate'
      WHEN c.exact_line_candidate_books < 2
        THEN 'insufficient_exact_line_candidate_books'
      ELSE 'candidate_only_not_designated'
    END AS close_coverage_state
  FROM scoped p
  JOIN close_coverage c USING (pick_id)
), readiness AS (
  SELECT *, CASE
    WHEN settlement_state <> 'settled_metadata_complete'
      THEN 'blocked_settlement'
    WHEN entry_state <> 'entry_metadata_ready'
      THEN 'blocked_entry_provenance'
    WHEN close_coverage_state <> 'candidate_only_not_designated'
      THEN 'blocked_close_coverage'
    ELSE 'blocked_independent_frozen_export_and_owner_close_mapping'
  END AS real_clv_readiness
  FROM classified
)
SELECT now() AS checked_at, mode, market, settlement_state, entry_state,
       close_coverage_state, real_clv_readiness, count(*) AS positions
FROM readiness
GROUP BY mode, market, settlement_state, entry_state,
         close_coverage_state, real_clv_readiness
ORDER BY mode, market, settlement_state, entry_state,
         close_coverage_state, real_clv_readiness;
