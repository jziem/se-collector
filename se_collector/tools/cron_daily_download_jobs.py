import logging
import os
from datetime import date, datetime, time, timedelta
from typing import Sequence

import requests
from bs4 import BeautifulSoup

TARGET_FOLDER = "/data/"
SITE = "https://www.ls-x.de"
SITE_WITH_PDF_LINKS = f"{SITE}/de/kursblatt"
SITE_TRADES_YESTERDAY = f"{SITE}/_rpc/json/.lstc/instrument/list/lsxtradesyesterday"
SITE_NONE_TRADING_DAYS = f"{SITE}/de/wissen"
_se_twd: [int] = [1, 2, 3, 4, 5]  # stock exchange trading week days, ISO calendar week day
_se_tt: [time, time] = [time(7, 30), time(23, 0)]  # stock exchange trading hours, in local time zone


def download(url: str, filename: str):
    tf = f"{TARGET_FOLDER}{filename}"  # target file
    if not os.path.exists(tf):
        req = requests.get(url, allow_redirects=True)
        if req.ok:
            logging.info(f"downloading {url} to {tf}")
            with open(tf, 'wb') as f:
                f.write(req.content)


def _get_none_trading_days(url: str = SITE_NONE_TRADING_DAYS) -> Sequence[date]:
    """
    Get none working days, documented at LS-X.
    :return: optional array of dates or none.
    """
    r = requests.get(url)
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


def download_stock_market_reports():
    # get (historical) daily reports
    logging.info(f"stock market report sync started.")
    r = requests.get(SITE_WITH_PDF_LINKS)
    if r.ok:
        soup = BeautifulSoup(r.content, 'html5lib')
        pdf = [(f"{SITE}{a['href']}", a['href'].split("/")[-1]) for a in soup.find_all('a', href=True)
               if a['href'].endswith('pdf')]
        for p in pdf:
            download(p[0], p[1])
    logging.info(f"stock market report sync done.")


if __name__ == '__main__':
    # download all pdf data and csv available.
    # if yesterday was trading day, let's download the complete trading history of that day.
    logging.info("start historical trade data sync.")
    try:
        rd = datetime.now()
        yesterday = datetime(rd.year, rd.month, rd.day, 12, 0) - timedelta(days=1)
        if _ts_in_working_hours(yesterday):
            download(SITE_TRADES_YESTERDAY, f"lsx_trades_{yesterday.strftime('%Y%m%d')}.csv")
        download_stock_market_reports()
        logging.info("end of historical trade data sync.")
    except:
        logging.error("Failed to synchronize historical trade data.", exc_info=True)
