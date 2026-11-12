# NBA Cup publisher benchmark verification

The [revision-pinned publisher README](https://huggingface.co/datasets/minmaxxxer/nba-closing-lines-ats-2025-26/blob/e0e54a9cb67ae1264068652b9a34523ab64e48ee/README.md) was retrieved as documentation only. Its SHA-256 is
`af3dc992e29bed634a6d53eaaa567eea39c7977168b4c92bd9fa85d04ec95bb1`.
The publisher describes proportional margin removal by bookmaker followed
by a brand-level median. It defines the closing over probability as no-vig
and the full-game totals and final scores as overtime-inclusive. The closing
grade uses over, under or push, and its margin is combined points minus the
closing total. These definitions match the registered endpoint and the
implemented recomputation.

The documentation describes the compilation. It cannot verify individual
books' rules, source independence or decision-time price availability. The
overtime flag still lacks the explicit encoding required by the separate
prior-overtime protocol. The Cup diagnostic does not use that flag.

Before evaluation, retain authoritative 2025 Cup rules, all 60 group
fixtures and the independent regular-season schedule for the same dates.
The fixture crosswalk still needs exact sides and start verification.
No outcomes, raw dataset rows or subgroup counts were read for this review.
The protocol and source data remain unchanged.
