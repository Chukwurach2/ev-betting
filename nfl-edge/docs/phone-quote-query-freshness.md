# Filter phone quote times before deduplication

The SQL query bounds source time to the previous 15 minutes, excluding the
expiry boundary and future timestamps, before choosing the newest row for
each selection. This prevents a future-dated row from hiding an otherwise
fresh quote for the same operator and contract. JavaScript still checks
freshness when building cards after the query.

The regression checks the SQL freshness predicate and newest-row ordering.
It fails on the previous route. The effect was reproduced with the query
contract locally. No production database query, odds request or deployment
was performed, and there is no evidence that a production row was affected.
