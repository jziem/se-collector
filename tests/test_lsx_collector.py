from datetime import date, datetime, timedelta

from se_collector.lsx.lsx_collector import _ts_in_working_hours, _get_none_trading_days, _get_trades


def test__get_none_trading_days():
    none_trading_days = _get_none_trading_days()
    # expect at least new year and xmas as holiday
    assert date(datetime.today().year, 12, 24) in none_trading_days
    assert date(datetime.today().year, 12, 31) in none_trading_days
    assert date(datetime.today().year + 1, 1, 1) in none_trading_days


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
    assert len(_get_trades())>0
