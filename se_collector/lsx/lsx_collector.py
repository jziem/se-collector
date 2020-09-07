"""
Get all trades.
"""
import logging
from datetime import time, date, datetime, timedelta
from functools import lru_cache
from typing import Optional, Sequence
from bs4 import BeautifulSoup
import csv

import requests

_URL_YESTERDAY: str = "https://www.ls-x.de/_rpc/json/.lstc/instrument/list/lsxtradesyesterday"
_URL_NOW: str = "https://www.ls-x.de/_rpc/json/.lstc/instrument/list/lsxtradestoday"
_se_tt: [time, time] = [time(7, 30), time(23, 0)]  # stock exchange trading hours, in local time zone
_se_twd: [int] = [1, 2, 3, 4, 5]  # stock exchange trading week days, ISO calendar week day

class TradeData:
    isin:str
    displayName:str
    time:datetime
    price:float
    volume:int

@lru_cache
def _get_none_trading_days() -> Sequence[date]:
    """
    Get none working days, documented at LS-X.
    :return: optional array of dates or none.
    """
    url = "https://www.ls-x.de/de/wissen"
    if r := requests.get(url):
        if r.ok:
            # lsx show none working days with a single table at above url.
            try:
                return [datetime.strptime(the_date, "%d.%m.%Y").date() for row in
                        BeautifulSoup(r.content, 'html5lib').find('table').tbody.find_all("tr") for the_date in
                        row.td]
            except:
                logging.error("Unable to parse response of LSX none working days.", exc_info=True)
    logging.warning("no data.")
    return []


def _ts_in_working_hours(d: datetime) -> bool:
    """
    Test if datetime is within working hours.
    :param d: date to test
    :return: True if date is a working day
    """
    return d.isoweekday() in _se_twd and _se_tt[0] <= d.time() <= _se_tt[1] and d.date() not in _get_none_trading_days()


def _get_csv(today: bool) -> Optional[str]:
    pass


def _get_yesterday(d:datetime)-> Sequence[(str, str, datetime, float, int)]:
    """
        Get current trades
        :param rd:
        :return: [(ISIN, display name, timestamp, price, volume), ...]
        """
    # pdwh = previous day within working hours
    pdwh = datetime(rd.year, rd.month, rd.day, 12, 0) - timedelta(days=1)
    if _ts_in_working_hours(pdwh):
        # only possible if previous day was is a working day
        if r := requests.get(_URL_YESTERDAY):
            if r.ok:
                reader = csv.DictReader(r.content)
                return [TradeData() for r in reader]


# @lru_cache(maxsize=1)
def _get_trades() -> Sequence[(str, str, datetime, float, int)]:
    """
    Get current trades
    :param rd:
    :return: [(ISIN, display name, timestamp, price, volume), ...]
    """
    # pdwh = previous day within working hours
    pdwh = datetime(rd.year, rd.month, rd.day, 12, 0) - timedelta(days=1)
    if _ts_in_working_hours(pdwh):
        # only possible if previous day was is a working day
        if r:=requests.get(_URL_YESTERDAY):
            if r.ok:
                reader=csv.DictReader(r.content)
                return [TradeData() for r in reader]

def update():
    requests.get("https://www.ls-x.de/_rpc/json/.lstc/instrument/list/lsxtradestoday")
