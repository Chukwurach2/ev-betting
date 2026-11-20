# NBA Cup weighted estimator and uncertainty

The aggregate weights each comparable week by its Cup game count. The fixed
coverage gates require 40 Cup games, 50 other games and four comparable weeks.
Empty and one-group weeks remain in the calendar with zero weight. The
two-week circular bootstrap truncates each sample to its original length.
It uses 200,000 attempts, seed 20261111 and the registered linear 99.9% interval.
At least 190,000 draws must be valid.

The positive screen requires a contrast of at least three percentage points
and a strictly positive lower interval endpoint. The shared sampling helper
preserves the NHL diagnostic's four-week default. All 34 local synthetic
tests passed, including five new estimator tests.

Historical evaluation requires reviewed rules and fixtures, a complete
regular-schedule crosswalk and verified publisher grading semantics. The
protocol remains frozen. This implementation has only used synthetic fixtures.
