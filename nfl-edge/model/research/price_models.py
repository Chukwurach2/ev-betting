"""Price-model protocol for market microstructure families.

Score-prediction families (see families.py) are evaluated by the historical
replay lab because nflverse gives us decades of scores and closing lines.
Price models are different: their features (line movement, book disagreement,
stale prices) only exist in forward quote captures (nfl_edge_odds_quotes),
and historical intraday quotes do not exist to fabricate. So price models
validate on FORWARD data -- fair price vs the eventual no-vig closing price,
CLV-style -- never in replay.py.

Protocol:
    PriceModel.fair_prob(features: dict) -> float | None
        features: a build_features() vector from market_features.py.
        Returns a no-vig fair probability for the selection, or None when
        the feature vector is too thin to price.

A price family graduates only on forward evidence: positive mean CLV vs the
no-vig close, acceptable calibration, and enough independent wagers. It is
registered here, NOT in the replay families registry, to prevent anyone
from accidentally "validating" it on data that cannot support it.
"""
from __future__ import annotations


class MarketOnlyV1:
    """Stub: market-only price discovery, forward-data only.

    Hypothesis: exploitable information lives more in sportsbook price
    formation (movement, disagreement, staleness) than in predicting NFL
    scores better than the market. Uses NO team-strength or game-outcome
    features -- only the microstructure features in market_features.py.

    Status: not implemented. It must accumulate legitimate forward evidence
    from nfl_edge_odds_quotes before any fair_prob exists to evaluate.
    """
    name = "market-only-v1"

    def fair_prob(self, features: dict) -> float | None:
        """Return a no-vig fair probability, or None if unpriceable."""
        raise NotImplementedError(
            "forward-data only: no historical intraday quotes exist; "
            "market-only-v1 accumulates evidence from nfl_edge_odds_quotes "
            "and validates fair vs closing no-vig (CLV-style), not in "
            "replay.py")


PRICE_FAMILIES: dict[str, type] = {
    MarketOnlyV1.name: MarketOnlyV1,
}


def price_family_names() -> list[str]:
    """Registered price-model family names."""
    return sorted(PRICE_FAMILIES)


def get_price_family(name: str):
    """Fetch a price family class by name, else raise ValueError."""
    try:
        return PRICE_FAMILIES[name]
    except KeyError:
        raise ValueError("unknown price family %r (have %s)"
                         % (name, price_family_names()))
