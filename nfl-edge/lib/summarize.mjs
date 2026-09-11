// Pure, testable track-record math for the shadow engine. No I/O, no secrets.
// Rows are settled shadow picks: {result, stake_units, decimal_odds,
// consensus_fair_prob, edge, clv_prob_points, settled_at, market}.

export function profitOf(p) {
  const stake = Number(p.stake_units);
  if (p.result === 'win') return stake * (Number(p.decimal_odds) - 1);
  if (p.result === 'loss') return -stake;
  return 0; // push: stake returned
}

export function summarize(rows) {
  const n = rows.length;
  const decided = rows.filter((p) => p.result !== 'push');
  const wins = rows.filter((p) => p.result === 'win').length;
  const staked = rows.reduce((a, p) => a + Number(p.stake_units), 0);
  const pl = rows.reduce((a, p) => a + profitOf(p), 0);
  const clvs = rows.map((p) => Number(p.clv_prob_points)).filter(Number.isFinite);
  // Drawdown on cumulative shadow P&L in settlement order.
  let cum = 0, peak = 0, maxDd = 0;
  const equity = [];
  for (const p of rows) {
    cum += profitOf(p);
    peak = Math.max(peak, cum);
    maxDd = Math.min(maxDd, cum - peak);
    equity.push({t: p.settled_at, cum: Math.round(cum * 1000) / 1000});
  }
  // Calibration buckets on consensus fair probability (pushes excluded).
  const buckets = [
    {label: '40–50%', lo: 0.4, hi: 0.5, n: 0, pred: 0, wins: 0},
    {label: '50–60%', lo: 0.5, hi: 0.6, n: 0, pred: 0, wins: 0},
    {label: '60–70%', lo: 0.6, hi: 0.7, n: 0, pred: 0, wins: 0},
    {label: '70%+', lo: 0.7, hi: 1.01, n: 0, pred: 0, wins: 0},
  ];
  for (const p of decided) {
    const fp = Number(p.consensus_fair_prob);
    const b = buckets.find((x) => fp >= x.lo && fp < x.hi);
    if (!b) continue;
    b.n += 1;
    b.pred += fp;
    if (p.result === 'win') b.wins += 1;
  }
  // Per-market breakdown.
  const byMarket = {};
  for (const p of rows) {
    const m = byMarket[p.market] || (byMarket[p.market] = {n: 0, pl: 0, staked: 0});
    m.n += 1;
    m.pl += profitOf(p);
    m.staked += Number(p.stake_units);
  }
  return {
    settled: n,
    wins,
    losses: rows.filter((p) => p.result === 'loss').length,
    pushes: n - decided.length,
    win_rate: decided.length ? wins / decided.length : null,
    units_staked: Math.round(staked * 1000) / 1000,
    units_pl: Math.round(pl * 1000) / 1000,
    roi: staked ? pl / staked : null,
    avg_edge: n ? rows.reduce((a, p) => a + Number(p.edge), 0) / n : null,
    avg_clv_pp: clvs.length ? clvs.reduce((a, c) => a + c, 0) / clvs.length : null,
    positive_clv_rate: clvs.length ? clvs.filter((c) => c > 0).length / clvs.length : null,
    max_drawdown_units: Math.round(maxDd * 1000) / 1000,
    equity_curve: equity.slice(-200),
    calibration: buckets.map((b) => ({
      bucket: b.label,
      n: b.n,
      predicted: b.n ? b.pred / b.n : null,
      actual: b.n ? b.wins / b.n : null,
    })),
    by_market: Object.entries(byMarket).map(([market, m]) => ({
      market,
      n: m.n,
      units_pl: Math.round(m.pl * 1000) / 1000,
      roi: m.staked ? m.pl / m.staked : null,
    })),
  };
}
