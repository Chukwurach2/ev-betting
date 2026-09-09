# NFL Edge V2.4 production completion

This package keeps V2.3 UI/auth behavior and adds the missing production-engine components without fabricating a champion.

1. Train and chronologically validate the existing drive model from nflverse PBP using `model/train_validate.py`. A model cannot be marked production-ready without `validation.json` and artifact hashes.
2. `model/provider_sgo.py` is a SportsGameOdds adapter. It requires `SPORTSGAMEODDS_API_KEY`, keeps original quote timestamps, filters to mapped NY books, and ignores unsupported/unmapped contracts. First-drive markets must be mapped only after exact provider IDs are observed in a real response.
3. `ops/readiness.py` fails closed unless both a validated artifact and fresh mapped provider quotes exist.
4. `ops/schema_v24.sql` is an additive migration for deterministic snapshot identity, provider timestamps, settlement rules, and run receipts. It is not applied by this package.
5. V2.3 public UI remains data-driven through Neon and should not be redeployed merely for data changes.

No model version is promoted and no pick is emitted by this package until validation and feed checks pass.

## Free odds provider update
Primary provider is now The Odds API free tier using `THE_ODDS_API_KEY`. The scheduler should call the free NFL events endpoint for discovery and only call event markets/odds when a game is inside a due decision checkpoint. `provider_oddsapi.py` preserves bookmaker observation time and API quota headers. Exact unsupported first-drive/first-5-minute contracts remain unsupported until observed and mapped from a real provider response.
