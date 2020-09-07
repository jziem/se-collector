"""
Get all trades.
"""
import io
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
    def __init__(self, isin: str, display_name: str, timestamp: datetime, price: float, volume: int):
        self.isin = isin
        self.display_name = display_name
        self.timestamp: datetime = timestamp
        self.price: float = price
        self.volume: int = volume

    @staticmethod
    def from_str(isin: str, display_name: str, timestamp: str, price: str, volume: str):
        return TradeData(isin.strip(), display_name.strip(), datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f"),
                         float(price.replace(",", ".")), int(volume))


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


def _fetch_data(url):
    if r := requests.get(url):
        if r.ok:
            return r.content.decode("utf-8")


# @lru_cache(maxsize=1)
def _get_trades() -> Sequence[TradeData]:
    # pdwh = previous day within working hours
    trades: [TradeData] = []
    rd = datetime.now()
    pdwh = datetime(rd.year, rd.month, rd.day, 12, 0) - timedelta(days=1)
    # if previous day was a working day, include this request too.
    urls = [_URL_YESTERDAY, _URL_NOW] if _ts_in_working_hours(pdwh) else [_URL_NOW]
    for u in urls:
        with io.StringIO(_fetch_data(u), newline="\r\n") as f:
            trades.extend([TradeData.from_str(r["isin"], r["displayName"], r["time"], r["price"], r["size"]) for r in
                           csv.DictReader(f, delimiter=';', quoting=csv.QUOTE_ALL)])
    return trades
