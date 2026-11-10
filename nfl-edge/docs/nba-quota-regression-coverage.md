# NBA quota regression coverage

The existing NBA collector already requires a known free-events quota header, reserve50 and an explicit bounded opt-in request. This change does not alter those runtime rules or start collection.

The focused PR workflow previously ran Python NFL quota tests and selected Node checks, but omitted the existing NBA collection/feed tests. Add those existing suites and three synthetic collection cases: fixed one-region/two-market/operator request scope, malformed/unsafe quota-header refusal, and no retry or credential-URL disclosure after a failed odds request.

All inputs and credentials in these tests are synthetic;the fetcher is replaced before any request. Hosted tests consume no provider credits and need no provider/database secrets. This is regression coverage,not a shared monthly allocator or authorization to dispatch collection. Existing frozen NFL model,isolated NBA storage,bet journal and schedules remain unchanged.

This focused increment supports safe future quote coverage while source/data gates remain unresolved. No new strategy experiment,odds capture,paid polling,deployment or current-pick claim.

Validation history: initial hosted run42857414070/job129974120916 passed12of13NBA checks but failed the new whitespace assertion. Standard Headers normalizes surrounding whitespace before get();the test accidentally expected raw input rejection after normalization. Preserve that failed run. Correct the fixture to supply explicit raw malformed values and add a separate real-Headers normalization case. No runtime quota guard was changed or weakened.
