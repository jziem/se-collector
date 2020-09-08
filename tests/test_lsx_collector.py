from datetime import date, datetime, timedelta

import pytest
from urllib3.exceptions import MaxRetryError

from se_collector.lsx.lsx_collector import _ts_in_working_hours, _get_none_trading_days, _get_trades, TradeData, \
    _URL_NONE_TRADING_DAYS


def test__get_none_trading_days():
    none_trading_days = _get_none_trading_days()
    # expect at least new year and xmas as holiday
    assert date(datetime.today().year, 12, 24) in none_trading_days
    assert date(datetime.today().year, 12, 31) in none_trading_days
    assert date(datetime.today().year + 1, 1, 1) in none_trading_days
    # test for unreadable content
    with pytest.raises(Exception):
        _get_none_trading_days("http://localhost/nothing")

    # assert unparseable data:
    assert len(_get_none_trading_days("http://github.com"))==0


def test__ts_in_working_hours():
    # xmas
    assert not _ts_in_working_hours(datetime(datetime.today().year, 12, 24, 20, 0))
    # standard date but outside of working hours
    year_start = datetime(datetime.today().year, 1, 2, 23, 30)
    if not 0 < year_start.isoweekday() <= 5:
        year_start = year_start + timedelta(days=2)
    assert not _ts_in_working_hours(year_start)
    # same standard date within working hours
    year_start = year_start - timedelta(hours=8)  # 15:30 is within working hours
    assert _ts_in_working_hours(year_start)


def test__get_trades():
    assert len(_get_trades()) > 0


def test_trade_data():
    now = datetime.today()
    plus_1s = now + timedelta(seconds=1)
    # test if two trades with same data are valid
    assert TradeData("1", "test", now, 1.0, 1) == TradeData("1", "test", now, 1.0, 1)
    # display name does not matter
    assert TradeData("1", "changed DN", now, 1.0, 1) == TradeData("1", "test", now, 1.0, 1)
    # last compared value price is changed:
    assert TradeData("1", "changed DN", now, 2.34, 1) != TradeData("1", "test", now, 1.0, 1)
    # fast fail: not the same object:
    assert TradeData("1", "changed DN", now, 2.34, 1) != "something completely different."
