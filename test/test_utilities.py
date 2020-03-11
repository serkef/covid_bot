import datetime

from gsheet_bot.utilities import create_status, get_hashtag_country
import pytest


def test_status_template():
    """ Test status has correct message """

    day = datetime.date.today()

    status = create_status(total=1, value=1, country="Mordor", day=day)
    assert "First case reported in" in status
    assert "Raises total" not in status

    status = create_status(total=5, value=5, country="Mordor", day=day)
    assert "First 5 cases reported in" in status
    assert "Raises total" not in status

    status = create_status(total=5, value=1, country="Mordor", day=day)
    assert "A new case reported today in" in status
    assert "Raises total " in status

    status = create_status(total=5, value=2, country="Mordor", day=day)
    assert "2 new cases reported today in" in status
    assert "Raises total" in status

    status = create_status(total=1234, value=2, country="Mordor", day=day)
    assert "#mordor" in status


def test_get_hashtag_country():
    cases = [
        {"given": "Germany", "expected": "#germany"},
        {"given": "Hong Kong", "expected": "#hongkong"},
        {"given": " A 123 day at Hong Kong", "expected": "#adayathongkong"},
        {"given": "1 2 3", "expected": "1 2 3"},
    ]

    for case in cases:
        assert case["expected"] == get_hashtag_country(case["given"])


if __name__ == "__main__":
    pytest.main([__file__])
