"""Challenger model: Elo-based team strength for NFL pregame prediction.

Versioned, deterministic, and gated. The model produces win probability,
predicted margin, and predicted total from offensive/defensive Elo ratings
trained on nflverse game data (1999-present, closing lines included).

Promotion policy: the model only graduates from shadow/research use if its
validation beats the market (closing lines) on spread AND total cover
prediction. As of v1 it does not, so it stays a research signal.
"""
from .elo import Phi, expected_scores, init_ratings, update_ratings
from .infer import Challenger, load_challenger

__all__ = ["Phi", "expected_scores", "init_ratings", "update_ratings",
           "Challenger", "load_challenger"]
