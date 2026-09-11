"""Market registry: the canonical list of bettable markets and their status.

In-memory configuration, not database state. Statuses change rarely and
always through an explicit governance decision, so they live in code where
the transition rules are enforced (see set_status). A future migration may
persist this to a table (e.g. nfl_edge_markets(key PK, status, updated_at))
for audit history; the transition rules here would remain the source of
truth for validity.
"""
