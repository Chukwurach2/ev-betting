# Tennessee–Dallas opener coverage

The [scheduled collector run](https://github.com/Chukwurach2/ev-betting/actions/runs/44386007902)
at 19:58 UTC November 18 reported one capture, one odds request and
two budgeted credits. A read-only database check verified 16 normalized
opener rows for Tennessee at Dallas, completed at 19:59:09.261 UTC.
Source timestamps ranged from 19:58:48 to 19:59:05 UTC.

Replaying the production card algorithm at that completion time found
all 16 rows fresh and produced zero entries. Half-point spread
selections had one operator per exact line. Half-point totals had at
most two operators per exact line. No supported offered contract
therefore had the two additional distinct reference operators required
by the current card rules. Integer lines in the snapshot have
unsupported push semantics for this report.

This is a coverage shortfall. It supplies no estimated positive or
negative edge for a qualifying card. The source prices are historical,
and the replay remains outside the prospective ledger. Current public
responses at 21:56 UTC listed 16 NFL and 12 NBA games with zero fresh
entries. Provider quota remaining and pre-expiry rendered phone output
were unverified.
