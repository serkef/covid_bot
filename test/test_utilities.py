import datetime

from gsheet_bot.utilities import create_status
import pytest


def test_status_template():
    """ Test status has correct message """

    # Given
    given = [
        {
            "input": {"value": 1, "territory": "Mordor", "day": datetime.date.today()},
            "expected": "1 case ",
        },
        {
            "input": {"value": 2, "territory": "Mordor", "day": datetime.date.today()},
            "expected": "2 cases ",
        },
    ]

    # Do
    for case in given:
        status = create_status(**case["input"])

        # Assert
        assert case["expected"] in status


if __name__ == "__main__":
    pytest.main([__file__])
