# Historical NBA data feasibility follow-up — 2026-10-26

Primary-source documentation review only; no account, API call, historical
odds download or model evaluation occurred.

## Candidate: HoopsAPI

https://hoopsapi.com/features/historical-odds-api
The provider advertises archived closing odds on every plan, including Free
(10 requests/day), and full timestamped price changes on its Data plan. It
illustrates an October2026 FIBA game, not a confirmed multi-season NBA archive.
Neither archive start date nor completeness for the registered2018–2026 NBA
windows is established by that example. Do not infer league/season coverage.

https://hoopsapi.com/legal/terms (displayed updated April13,2026)
The published terms require an account and confidential API key. They prohibit
redistribution/resale/sublicensing of raw odds, competing odds aggregation,
and bypassing authentication/rate limits. The license is limited to own use;
accuracy and continuous availability are not guaranteed.

Consequences for this project:
- No raw response or bookmaker table may be committed to GitHub or assumed safe
  for phone redistribution. Any derived-analysis delivery needs a separate
  permission/scope assessment before integration.
- Do not create an account, accept terms, collect keys, or buy a plan as part of
  this documentation review. No existing authorized connection was identified.
- Even if free closing odds become available, they cannot replace T-60 quotes in
  nba-b2b-rest-spread-v1. A closing-only baseline needs a separate preregistration
  with labels that do not claim earlier execution or point-in-time injury inputs.
- Before any test, establish authorized access, archive coverage, official-game
  mapping, quote timestamp semantics, decision-time feature availability and
  applicable contract rules. Fingerprint the input before viewing outcomes.

Status remains registered_data_blocked. No additional restriction is inferred
for an otherwise authorized source; these findings apply to this candidate's
published terms. Search for other permitted historical exports can continue.
