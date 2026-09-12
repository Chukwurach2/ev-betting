# Amendment A1 to market-alpha-v1 (PRE-RESULTS correction)

Date: 2026-09-12.
Status of real data: **no real-data analysis has been run.** The flaw below was
found by null simulation during implementation testing, before any execution
against `/tmp/quotes.csv` or Neon. This amendment is committed before results.

## Flaw found in the preregistered Track B primary test

The preregistered Track B primary test was a one-sided one-sample t-test of
mean game-level stale-side edge versus 0. Null simulation (true price fixed at
0.50 for every snapshot — a martingale with no drift — and every book's quote
equal to truth plus iid N(0, 0.01) noise, 120 synthetic games) shows the test
is **miscalibrated under the noise null**:

- mean game-level edge = 0.0182, 95% CI [0.0165, 0.0199]
- t-test p = 4.2e-98, hit rate = 0.970, hit binomial p = 2.6e-25

The test rejects with overwhelming "significance" when every deviation is pure
noise. Root cause is selection bias, not a coding bug: the rule "bet the side
where the book's quote is most generous versus the consensus" systematically
takes better-than-fair prices even when deviations are iid noise (the
line-shopping / minimum-quote effect). The conditional expectation of the edge
given |deviation| >= 0.02 is positive by roughly the threshold itself. The
preregistered null ("mean edge = 0 under no staleness") is therefore false even
when there is no staleness, and a "significant" result would be spurious.

Track A is NOT affected: under the null of independent noise, follower
agreement with a Pinnacle move is exactly 50% (selection is on the leader's
move; the follower's response is independent of it), so the binomial tests
remain calibrated. No change to Track A.

## Corrected Track B primary inference (replaces the t-test)

The preregistered t-test measured the profitability of "bet the
consensus-deviant quote" but cannot distinguish staleness from the
line-shopping / minimum-quote selection effect. The corrected primary test
targets the staleness MECHANISM directly — a stale book's quote lags the
market, so its deviation points opposite the market's move — with an
exactly calibrated null.

**Directional lag test (split consensus).** For each (game, market,
snapshot s strictly between the first and the closing snapshot, book b,
line L), all at exact line L (Amendment A1 exact-line rule):

- Let others(b) be books ≠ b with a valid pair at line L at s, sorted by
  book key. Require ≥ 4. Split deterministically: A = first 2, B = next 2.
- dev_b = f_b(s,L) − median{f(s,L) : books in A}. **Stale flag:**
  |dev_b| ≥ Y = 0.02 (unchanged threshold).
- Market move D = median{f(s,L) : books in B} − median{f(s−1,L) : books in
  B}; require B's books present at s−1 and |D| ≥ 0.01. (A and B are
  disjoint, so dev_b and D are independent under the null; the evaluated
  book never enters either consensus.)
- **Lag indicator:** sign(dev_b) == −sign(D) — the book's deviation points
  backward, toward the past consensus.

**Primary test:** one-sided exact binomial test of the lagging fraction
versus 0.5 at α = 0.05. Under the null (synchronous updating; deviations
are symmetric noise independent of market moves), dev_b is symmetric
around 0 and independent of D, so the lagging probability is exactly 0.5
— verified by null simulation (10 seeds, pure noise + market trend:
p-values 0.20–0.96, 0/10 false rejections). With a planted lagging book
the test rejects decisively (p ~ 1e-37, lagging fraction 1.0).

**Profitability (co-requirement, descriptive):** for lagging units, score
the stale-side bet against the closing LOBO consensus at the same line
(same edge definition as preregistered). Report the mean game-level edge
and 95% CI. The "stale_price_edge" verdict additionally requires mean
edge ≥ 0.01 (practical bar unchanged). The mean edge is reported with the
caveat that, as a CLV-vs-close measure, it partly reflects the
line-shopping selection effect; the directional test is what establishes
the staleness mechanism.

**Decision rule:** "stale_price_edge" requires directional p < 0.05 AND
mean lagging-cell edge ≥ 0.01. Directional p < 0.05 but edge < 0.01 →
"significant_but_negligible". Otherwise → "no_stale_price_signal".
Fewer than 30 valid flagged units → "infeasible" (feasibility floor).

The original t-test statistics are still computed and reported for
transparency, labeled "superseded by Amendment A1 — not used for the
verdict." Point-in-time safety holds: the flag uses snapshot s only; D
uses s and s−1; the closing benchmark is ex-post only.

## Transparency

The original (miscalibrated) t-test statistics will still be computed and
reported in the results document for transparency, clearly labeled as
"superseded by Amendment A1 — not used for the verdict." The verdict uses the
directional lag test (mechanism) plus the practical profitability bar. Unit
tests lock in: (i) the naive t-test rejects under pure noise (bias diagnosis);
(ii) the directional lag test does not reject under pure noise (calibration);
(iii) it rejects decisively with a planted lagging book (power); (iv) the
evaluated book never enters either consensus; (v) exact-line comparability.

## Clarification (no behavior change)

Track A multiplicity: the forward tests form one Holm family of 5 (the five
followers); the reverse falsification tests form a second, separate Holm
family of 5. This matches the implementation.

## Exact-line comparability for Track B (pre-results correction)

The preregistered Track B definitions evaluate each book "at its selected
line" — but different books' selected lines at the same snapshot can differ
(e.g. −3 vs −3.5), in which case |f_b(s) − C_{≠b}(s)| confounds contract
differences with staleness. Corrected rule, applied before any real-data
execution:

- A stale candidate is a (book b, line L) pair at snapshot s, using every
  valid two-sided line b offers at s (not only the selected modal line).
- The same-snapshot LOBO consensus C_{≠b,L}(s) is the median f over books
  b' ≠ b offering **exactly line L** at s; ≥ 2 such books required.
- The closing benchmark F_close is the median over books ≠ b offering
  **exactly line L** at the closing snapshot; ≥ 2 required (complete-case).
- The evaluated book never enters its own consensus (unchanged).

Everything else about Track B (stale threshold Y = 0.02, bet-side rule,
edge definition, game-level unit, rotation null, practical bar 0.01,
30-game feasibility floor) is unchanged.
