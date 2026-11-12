# NBA Cup full-game over residuals

The implementation gates on reviewed fixture coverage before inspecting final
scores. A supported contract has a positive half-point closing total and a
closing over probability between zero and one. The endpoint subtracts that
probability from the over indicator computed from the combined final score.
The recomputed side and score-minus-line margin must agree with the publisher
fields. Missing and unsupported benchmarks are counted separately by group.

The weekly inputs preserve every Monday-start Eastern week of the registered
window. They report group counts and the contrast only for weeks with both
groups. The weighted aggregate and two-week bootstrap remain to be implemented.
Authoritative schedule, rules, and publisher grading semantics are still
required before private evaluation. Five new synthetic cases passed alongside
the six existing schedule cases. No private rows were evaluated.
