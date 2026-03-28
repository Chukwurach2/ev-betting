from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


PLAYER_PROP_SPORT_ORDER = ["NBA", "NHL", "MLB"]

PLAYER_PROP_FRAMEWORKS_BY_SPORT: Dict[str, Dict[str, Any]] = {
    "NBA": {
        "sport_code": "NBA",
        "display_name": "NBA Player Props",
        "title": "NBA Player Props — Objective / Scope",
        "market_focus": [
            "Standard NBA player props",
            "Main markets: Points, Rebounds, Assists, P+A, P+R, R+A, PRA, 3PM, Steals",
        ],
        "allowed_prop_markets": [
            "Player Points",
            "Player Rbs",
            "Player Assists",
            "Player Stl",
            "Player P+A",
            "Player P+R",
            "Player R+A",
            "Player PRA",
            "Player 3PM",
        ],
        "allowed_books": ["Pinnacle", "Circa", "BetOnline", "DraftKings", "FanDuel"],
        "required_sharp_books": ["Pinnacle", "Circa", "BetOnline"],
        "preferred_books": ["DraftKings", "FanDuel"],
        "min_total_books": 3,
        "default_devig_method": "Split Weights",
        "devig_methodology": [
            "Additive devig",
            "Sharp-weighted pricing anchored toward sharper books",
            "Re-normalize weights based on whichever approved books are available",
        ],
        "default_weights": [
            ("Pinnacle", 0.50),
            ("Circa", 0.25),
            ("BetOnline", 0.20),
            ("DraftKings", 0.03),
            ("FanDuel", 0.02),
        ],
        "preferred_odds_band": "+105 to +165",
        "acceptable_odds_band": "+165 to +250",
        "baseline_ev_threshold_pct": 6.0,
        "high_odds_ev_threshold_pct": 10.0,
        "very_long_odds_ev_threshold_pct": 12.0,
        "odds_bands": [
            {
                "name": "Primary",
                "min_odds": 105,
                "max_odds": 165,
                "min_ev_pct": 6.0,
                "fit": "fit",
                "label": "Primary range",
                "explanation": "This fits the core NBA range for standard player props.",
            },
            {
                "name": "Extended",
                "min_odds": 166,
                "max_odds": 250,
                "min_ev_pct": 10.0,
                "fit": "borderline",
                "label": "Extended range",
                "explanation": "Longer NBA prices need a stronger EV edge than the primary range.",
            },
            {
                "name": "High Odds",
                "min_odds": 251,
                "max_odds": None,
                "min_ev_pct": 12.0,
                "fit": "borderline",
                "label": "High-odds exception",
                "explanation": "Very long NBA prices qualify only on a high-EV exception basis.",
            },
        ],
        "sections": [
            ("Devig Books", [
                "Pinnacle",
                "Circa",
                "BetOnline",
                "DraftKings",
                "FanDuel",
            ]),
            ("Sharp Confirmation / Book Requirements", [
                "Minimum 3 books total",
                "Accept if any of the following is true:",
                "Pinnacle present, OR",
                "Circa present, OR",
                "BetOnline present plus DraftKings or FanDuel",
                "Skip soft-book-only markets",
            ]),
            ("Betting Range / EV Guidance", [
                "Primary zone: +105 to +165 with minimum EV of +6%",
                "Extended zone: +166 to +250 with minimum EV of +10%",
                "High odds rule: +251 or longer only if EV is at least +12%",
            ]),
            ("Confidence / Quality Framing", [
                "Tier 1: Pinnacle or Circa anchoring with broader market confirmation",
                "Tier 2: BetOnline plus another preferred book",
                "Tier 3: weaker support / softer confirmation, requiring more caution",
            ]),
            ("Fair vs Market Gap Filter", [
                "Primary zone: minimum 8-cent gap",
                "Extended zone: minimum 20-cent gap",
                "High odds: minimum 30-cent gap",
                "Market consensus uses median of available book prices",
            ]),
            ("EV Alerts Queue", [
                "Use the EV Alerts page to review live candidates generated from these rules",
                "Alerts are reviewed manually before being logged",
                "Logging from EV Alerts adds an OPEN bet to the ledger",
                "Alerts are not automatically treated as placed bets",
            ]),
            ("Avoid", [
                "Placeholder / neutral rows",
                "Boost-driven outliers without sharp confirmation",
                "Soft-book-only edges",
                "Plays failing gap filter",
            ]),
        ],
    },
    "NHL": {
        "sport_code": "NHL",
        "display_name": "NHL Player Props",
        "title": "NHL Player Props — Objective / Scope",
        "market_focus": [
            "Standard NHL player props",
            "Main markets: Shots on Goal (SOG), Points, Assists",
            "Exclude anytime goals from the standard framework for now unless handled separately elsewhere",
        ],
        "allowed_prop_markets": [
            "Shots on Goal",
            "Player Points",
            "Player Assists",
        ],
        "allowed_books": ["Pinnacle", "Circa", "FanDuel", "DraftKings", "Caesars", "BetMGM"],
        "required_sharp_books": ["Pinnacle", "Circa"],
        "preferred_books": ["FanDuel", "DraftKings"],
        "min_total_books": 3,
        "default_devig_method": "Multiplicative",
        "devig_methodology": [
            "Use a conservative, weighted market-based devig approach",
            "For SOG / Points / Assists, use multiplicative as the primary standard method",
            "Weighted fair value should favor sharp books when available",
            "Re-normalize weights based on whichever approved books are available",
        ],
        "default_weights": [
            ("Pinnacle", 0.35),
            ("Circa", 0.30),
            ("FanDuel", 0.125),
            ("DraftKings", 0.125),
            ("Caesars", 0.05),
            ("BetMGM", 0.05),
        ],
        "preferred_odds_band": "+100 to +150",
        "acceptable_odds_band": "+151 to +185",
        "baseline_ev_threshold_pct": 5.0,
        "high_odds_ev_threshold_pct": 6.0,
        "odds_bands": [
            {
                "name": "Preferred",
                "min_odds": 100,
                "max_odds": 140,
                "min_ev_pct": 5.0,
                "fit": "fit",
                "label": "Preferred range",
                "explanation": "This is within the preferred NHL plus-money window for standard props.",
            },
            {
                "name": "Long Preferred",
                "min_odds": 141,
                "max_odds": 150,
                "min_ev_pct": 6.0,
                "fit": "fit",
                "label": "Preferred range with stricter EV",
                "explanation": "Longer NHL prices should clear a higher EV bar even inside the main target window.",
            },
            {
                "name": "Extended",
                "min_odds": 151,
                "max_odds": 185,
                "min_ev_pct": 6.0,
                "fit": "borderline",
                "label": "Extended range",
                "explanation": "This is playable only as a lower-confidence NHL fit with extra caution.",
            },
        ],
        "sections": [
            ("Devig Books", [
                "Pinnacle",
                "Circa",
                "FanDuel",
                "DraftKings",
                "Caesars",
                "BetMGM",
            ]),
            ("Book Requirements", [
                "Require at least 1 sharp book: Pinnacle OR Circa",
                "Require at least 3 books total",
                "Prefer FanDuel or DraftKings to be present",
                "If requirements are not met, mark the market as weaker or below preferred quality",
            ]),
            ("Betting Range / EV Guidance", [
                "Focus mainly on plus-money props, generally around +100 to +150",
                "Require a meaningful EV edge, with a baseline of about +5%",
                "Prefer a higher EV threshold as odds get longer",
                "Use extra caution once prices move outside the core plus-money band",
            ]),
            ("Confidence / Quality Framing", [
                "Tier 1: Pinnacle + multiple books present",
                "Tier 2: Circa + multiple books present",
                "Tier 3: weaker support / fewer preferred books, requiring more caution",
            ]),
        ],
    },
    "MLB": {
        "sport_code": "MLB",
        "display_name": "MLB Player Props",
        "title": "MLB Player Props — Objective / Scope",
        "market_focus": [
            "Standard MLB player props only",
            "Examples: pitcher strikeouts, outs recorded, hits allowed, earned runs, walks allowed, batter hits, total bases, RBI, runs, H+R+RBI",
            "Exclude home run props from the standard MLB framework for now",
        ],
        "allowed_prop_markets": [
            "Player Strikeouts",
            "Player Outs Recorded",
            "Player Earned Runs",
            "Player Hits Allowed",
            "Player Walks Allowed",
            "Player Hits",
            "Player Total Bases",
            "Player RBI",
            "Player Runs",
            "Player H+R+RBI",
        ],
        "allowed_books": ["Pinnacle", "Circa", "FanDuel", "DraftKings", "Caesars", "BetMGM"],
        "required_sharp_books": ["Pinnacle", "Circa"],
        "preferred_books": ["FanDuel", "DraftKings"],
        "min_total_books": 3,
        "default_devig_method": "Multiplicative",
        "devig_methodology": [
            "Use multiplicative devig as the primary method for standard MLB player props",
            "Weighted fair value should be anchored toward sharper books",
            "Re-normalize weights based on whichever required books are available",
        ],
        "default_weights": [
            ("Pinnacle", 0.35),
            ("Circa", 0.30),
            ("FanDuel", 0.125),
            ("DraftKings", 0.125),
            ("Caesars", 0.05),
            ("BetMGM", 0.05),
        ],
        "preferred_odds_band": "+100 to +150",
        "acceptable_odds_band": "+151 to +185",
        "baseline_ev_threshold_pct": 5.0,
        "high_odds_ev_threshold_pct": 6.0,
        "odds_bands": [
            {
                "name": "Preferred",
                "min_odds": 100,
                "max_odds": 140,
                "min_ev_pct": 5.0,
                "fit": "fit",
                "label": "Preferred range",
                "explanation": "This is inside the standard MLB target range for non-HR player props.",
            },
            {
                "name": "Long Preferred",
                "min_odds": 141,
                "max_odds": 150,
                "min_ev_pct": 6.0,
                "fit": "fit",
                "label": "Preferred range with stricter EV",
                "explanation": "Longer MLB prices inside the main band should clear a stricter EV threshold.",
            },
            {
                "name": "Extended",
                "min_odds": 151,
                "max_odds": 185,
                "min_ev_pct": 6.0,
                "fit": "borderline",
                "label": "Extended range",
                "explanation": "This sits outside the core MLB range, so it should be treated as a lower-confidence fit.",
            },
        ],
        "sections": [
            ("Devig Books", [
                "Pinnacle",
                "Circa",
                "FanDuel",
                "DraftKings",
                "Caesars",
                "BetMGM",
            ]),
            ("Book Requirements", [
                "Require at least 1 sharp book: Pinnacle OR Circa",
                "Require at least 3 books total",
                "Prefer at least one of FanDuel or DraftKings to also be present",
                "If requirements are not met, treat the market as below preferred quality or filter it out based on app logic",
            ]),
            ("Betting Range / EV Guidance", [
                "Focus mainly on odds from +100 to +150",
                "Require at least +5% EV as baseline",
                "Prefer +6%+ EV once odds get longer than roughly +140",
                "Bet MLB standard player props when odds are +100 or better and EV is at least +5%; prefer +6%+ once odds get longer than about +140.",
            ]),
            ("Confidence / Quality Framing", [
                "Tier 1: Pinnacle + multiple books present",
                "Tier 2: Circa + multiple books present",
                "Tier 3: weaker support / fewer preferred books, requiring more caution",
            ]),
        ],
    },
}


def objective_scope_labels() -> List[str]:
    return [PLAYER_PROP_FRAMEWORKS_BY_SPORT[sport]["display_name"] for sport in PLAYER_PROP_SPORT_ORDER]


def framework_for_sport(sport: str) -> Dict[str, Any]:
    return PLAYER_PROP_FRAMEWORKS_BY_SPORT[sport]


def sport_from_display_name(display_name: str) -> Optional[str]:
    for sport in PLAYER_PROP_SPORT_ORDER:
        if PLAYER_PROP_FRAMEWORKS_BY_SPORT[sport]["display_name"] == display_name:
            return sport
    return None


def display_name_for_sport(sport: str) -> str:
    return PLAYER_PROP_FRAMEWORKS_BY_SPORT[sport]["display_name"]


def objective_scope_sections(sport: str) -> List[Tuple[str, List[str]]]:
    cfg = framework_for_sport(sport)
    sections: List[Tuple[str, List[str]]] = []
    if cfg.get("market_focus"):
        sections.append(("Market Focus", list(cfg["market_focus"])))
    if cfg.get("allowed_books"):
        sections.append(("Devig Books", list(cfg["allowed_books"])))
    if cfg.get("devig_methodology"):
        sections.append(("Devig Methodology", list(cfg["devig_methodology"])))
    if cfg.get("default_weights"):
        sections.append((
            "Suggested Default Book Weights",
            [f"{book}: {weight * 100:.1f}%" for book, weight in cfg["default_weights"]],
        ))
    sections.extend(cfg.get("sections", []))
    return sections


def evaluate_strategy_fit(sport: str, odds_american: float, ev_pct: float) -> Dict[str, Any]:
    cfg = framework_for_sport(sport)
    bands = cfg.get("odds_bands", [])

    if odds_american < 100:
        return {
            "fit": "outside",
            "indicator": "Outside Scope",
            "callout": "error",
            "reason": "Outside preferred betting range",
            "explanation": (
                f"This {cfg['display_name']} bet is outside scope because the current framework focuses on plus-money props, "
                f"and {odds_american:+.0f} is below the preferred range."
            ),
        }

    for band in bands:
        min_odds = band.get("min_odds")
        max_odds = band.get("max_odds")
        if odds_american < min_odds:
            continue
        if max_odds is not None and odds_american > max_odds:
            continue

        min_ev_pct = float(band["min_ev_pct"])
        if ev_pct >= min_ev_pct:
            fit = band["fit"]
            return {
                "fit": fit,
                "indicator": "Fits Objective / Scope" if fit == "fit" else "Borderline / lower-confidence fit",
                "callout": "success" if fit == "fit" else "warning",
                "reason": band["label"],
                "explanation": (
                    f"This {cfg['display_name']} bet fits because odds are {odds_american:+.0f} and EV is {ev_pct:.1f}%, "
                    f"which clears the {min_ev_pct:.1f}% threshold for the {band['label'].lower()}."
                ) if fit == "fit" else (
                    f"This {cfg['display_name']} bet is borderline because odds are {odds_american:+.0f} and EV is {ev_pct:.1f}%, "
                    f"which clears the {min_ev_pct:.1f}% threshold only in the {band['label'].lower()}."
                ),
            }
        return {
            "fit": "outside",
            "indicator": "EV below preferred threshold",
            "callout": "error",
            "reason": "EV below preferred threshold",
            "explanation": (
                f"This {cfg['display_name']} bet is outside scope because odds are {odds_american:+.0f}, "
                f"but EV is only {ev_pct:.1f}% and the framework calls for at least {min_ev_pct:.1f}% in this range."
            ),
        }

    highest_band = bands[-1] if bands else None
    if highest_band is not None:
        long_odds_threshold = float(highest_band["min_ev_pct"])
        if ev_pct >= long_odds_threshold:
            return {
                "fit": "borderline",
                "indicator": "Borderline / lower-confidence fit",
                "callout": "warning",
                "reason": "Outside preferred betting range",
                "explanation": (
                    f"This {cfg['display_name']} bet is outside the preferred odds band at {odds_american:+.0f}. "
                    f"EV of {ev_pct:.1f}% is strong enough to be noted, but it should still be treated as lower-confidence."
                ),
            }
        return {
            "fit": "outside",
            "indicator": "Outside preferred betting range",
            "callout": "error",
            "reason": "Outside preferred betting range",
            "explanation": (
                f"This {cfg['display_name']} bet is outside the preferred odds range at {odds_american:+.0f}, "
                f"and EV of {ev_pct:.1f}% does not justify the extra price risk."
            ),
        }

    return {
        "fit": "outside",
        "indicator": "Outside Scope",
        "callout": "error",
        "reason": "No matching strategy rule",
        "explanation": f"No strategy band was configured for {cfg['display_name']}.",
    }
