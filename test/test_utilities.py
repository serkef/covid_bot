import datetime

from gsheet_bot.utilities import create_status
import pytest


def test_status_template():
    """ Test status has correct message """

    day = datetime.date.today()

    status = create_status(total=1, value=1, territory="Mordor", day=day)
    assert "First incident reported for" in status
    assert "Raises total" not in status

    status = create_status(total=5, value=5, territory="Mordor", day=day)
    assert "First 5 " in status
    assert "Raises total" not in status

    status = create_status(total=5, value=1, territory="Mordor", day=day)
    assert "A new incident " in status
    assert "Raises total " in status

    status = create_status(total=5, value=2, territory="Mordor", day=day)
    assert "2 new incidents " in status
    assert "Raises total" in status


if __name__ == "__main__":
    pytest.main([__file__])
