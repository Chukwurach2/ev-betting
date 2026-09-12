import datetime as dt
import pathlib
import sys
import unittest

HERE = pathlib.Path(__file__).resolve()
OPS = HERE.parents[1] / "ops" if (HERE.parents[1] / "ops").exists() else HERE.parent
sys.path.insert(0, str(OPS))
import historical_outcomes as h


HEADER = ("game_id,season,week,game_type,gameday,home_team,away_team,"
          "home_score,away_score\n")


class ParseOutcomesTests(unittest.TestCase):
    def test_parses_settled_and_skips_unsettled(self):
        data = (HEADER +
                "2022_01_BUF_LA,2022,1,REG,2022-09-08,LA,BUF,10,31\n" +
                "2022_02_X_Y,2022,2,REG,2022-09-15,LA,BUF,NA,NA\n").encode()
        rows = h.parse_outcomes(data, (2022,))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["home_team"], "Los Angeles Rams")
        self.assertEqual(rows[0]["away_score"], 31)
        self.assertEqual(rows[0]["game_date"], dt.date(2022, 9, 8))

    def test_filters_seasons(self):
        data = (HEADER +
                "2021_01_BUF_LA,2021,1,REG,2021-09-08,LA,BUF,10,31\n").encode()
        with self.assertRaisesRegex(ValueError, "no settled outcomes"):
            h.parse_outcomes(data, (2022,))

    def test_unknown_team_fails_closed(self):
        data = (HEADER +
                "x,2022,1,REG,2022-09-08,XXX,BUF,10,31\n").encode()
        with self.assertRaisesRegex(ValueError, "unmapped team"):
            h.parse_outcomes(data, (2022,))

    def test_duplicate_game_id_fails_closed(self):
        row = "x,2022,1,REG,2022-09-08,LA,BUF,10,31\n"
        with self.assertRaisesRegex(ValueError, "duplicate game_id"):
            h.parse_outcomes((HEADER + row + row).encode(), (2022,))

    def test_negative_score_rejected(self):
        data = (HEADER + "x,2022,1,REG,2022-09-08,LA,BUF,-1,31\n").encode()
        with self.assertRaisesRegex(ValueError, "negative home_score"):
            h.parse_outcomes(data, (2022,))


if __name__ == "__main__":
    unittest.main()
