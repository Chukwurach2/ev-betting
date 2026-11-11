"""Outcome-blind fixture gate for the registered 2025 NBA Cup comparison."""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo('America/New_York')
FIRST = date(2025, 10, 31)
LAST = date(2025, 11, 28)


def identifier(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('Nonempty explicit fixture and team IDs required')
    return value


def identity(row):
    start = datetime.fromisoformat(row['start_time_utc'].replace('Z', '+00:00'))
    if start.utcoffset() != timedelta(0):
        raise ValueError('UTC fixture start required')
    day = start.astimezone(EASTERN).date()
    if row['date_et'] != day.isoformat():
        raise ValueError('Fixture start and Eastern date disagree')
    home, away = identifier(row['home_team_id']), identifier(row['away_team_id'])
    if home == away:
        raise ValueError('Fixture sides must be distinct')
    return home, away, start, day


def audit_fixtures(publisher_rows, schedule_crosswalk, cup_fixture_ids):
    """Return publisher ID to Cup membership from reviewed schedule inputs."""
    cup_ids = [identifier(value) for value in cup_fixture_ids]
    cup_ids_set = set(cup_ids)
    if len(cup_ids) != 60 or len(cup_ids_set) != 60:
        raise ValueError('Exactly 60 distinct official Cup group IDs required')

    publisher = {}
    team_starts = set()
    for row in publisher_rows:
        if row['season_type'] != 'regular' or row['season'] != '2025-26':
            continue
        home, away, start, day = identity(row)
        if not FIRST <= day <= LAST:
            continue
        game_id = identifier(row['game_id'])
        team_keys = {(home, start), (away, start)}
        if game_id in publisher or team_keys & team_starts:
            raise ValueError('Duplicate publisher fixture or team start')
        publisher[game_id] = (home, away, start, day)
        team_starts.update(team_keys)

    labels = {}
    official_ids = set()
    for row in schedule_crosswalk:
        official = identifier(row['official_fixture_id'])
        provider = identifier(row['publisher_game_id'])
        if official in official_ids or provider in labels:
            raise ValueError('Crosswalk must be one-to-one')
        if publisher.get(provider) != identity(row):
            raise ValueError('Crosswalk and publisher fixture identity disagree')
        official_ids.add(official)
        labels[provider] = official in cup_ids_set

    if labels.keys() != publisher.keys() or not cup_ids_set <= official_ids:
        raise ValueError('Incomplete official regular or Cup schedule coverage')
    return labels
