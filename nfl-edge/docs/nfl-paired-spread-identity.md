# NFL team identity in paired spread estimates

NFL quote rows can use full provider team names for spread selections while
the phone schedule uses abbreviations. After matching the game and kickoff,
the optional power de-vig diagnostic uses the existing exact NFL alias table
to identify the opposing team. It requires one opposing quote at the mirrored
line, exact source time and same book key. A third team, another kickoff or
ambiguous aliases leave the diagnostic unavailable.

The baseline still prices the stored selection verbatim. NBA keeps its
existing exact team-name and provider-event matching. No quote collection,
deployment or historical performance evaluation was triggered.
