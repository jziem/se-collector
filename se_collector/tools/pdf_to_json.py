import json
import math
import os
import re
from datetime import time, date, datetime
from glob import glob
from json import JSONEncoder
from locale import atof, atoi, setlocale, LC_NUMERIC
from multiprocessing.context import Process
from multiprocessing.spawn import freeze_support
from time import time as timemeasure

from PyPDF3 import PdfFileReader
from PyPDF3.pdf import PageObject


class JsonDateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()


def parse_kursblatt_page(page: PageObject,
                         share_name: str,
                         base_date: date,
                         shares: {str: [(time, int, float, int)]},
                         re_share_with_isin, re_time, transform_locale: bool = None) -> (str, date, bool):
    """
    parse a single page and:
    - guess the used locale (which switches over several pdf)
    - detect data lines without statemachine
    - return the updated last_share_name, updated base_date, detected_locale
    :param page: the page object
    :param share_name: last detected share name
    :param base_date: base date, in case that one pdf contains multiple dates
    :param shares: share dictionary
    :param re_share_with_isin: compiled regex (for faster multi process)
    :param re_time: compiled regex (for faster multi process)
    :param use_locale: 0 = DE ( "." = thousands, "," = decimal), 1 = US ( "," = thousands, "." = decimal)
    :return: share_name, base_date, locale_info (replace comma or not)
    """
    must_replace = transform_locale
    current_share = share_name
    local_date = base_date
    # split into separate items
    data = page.extractText().split("\n")
    i = 0
    while i < len(data):
        # find first TIME as it marks a complete row
        if len(data[i]) > 6 and data[i][0:6] == "Datum:":
            # parse new base date
            local_date = datetime.strptime(data[i][6:].strip(), "%d.%m.%Y").date()
            # print(f"updated localdate to {local_date}")
            i += 1
        elif re_time.match(data[i]):
            # workaround for jasper error "null" in volume field
            if data[i + 1] == "null":
                print("skipped error data line")
                i += 4
                continue
            # print("processing data line")
            if must_replace is None:
                # the sell or buy field should contain a number. and the -4 character is the identifier.
                must_replace = True if data[i + (3 if data[i + 2].strip() == '-' else 2)][-4] == "," else False
                print("detected locale is {}".format("DE" if must_replace else "US"))
            local_time = datetime.strptime(data[i].strip(), "%H:%M:%S").time()
            if data[i + 2] == '-':
                # entry is of type SELL
                if must_replace:
                    value = atof(data[i + 3].replace(".", "_").replace(",", ".").replace("_", ","))
                else:
                    value = atof(data[i + 3])
                ordertype = 0  # SELL = 0
            else:
                if must_replace:
                    value = atof(data[i + 2].replace(".", "_").replace(",", ".").replace("_", ","))
                else:
                    value = atof(data[i + 2])
                ordertype = 1  # BUY = 1
            if must_replace:
                volume = atoi(data[i + 1].replace(".", "_").replace(",", ".").replace("_", ","))
            else:
                volume = atoi(data[i + 1])  # .replace(".","").replace)
            entry = [datetime.combine(local_date, local_time), volume, value, ordertype]
            if current_share in shares.keys():
                shares.get(current_share).append(entry)
            else:
                shares.update({current_share: [entry]})
            i += 4  # skip to next line
        elif data[i] in ('Kursblatt', 'Uhrzeit', 'Kauf', 'Verkauf', 'Volumen'):  # ignored tags!
            # print(f"ignored tag {data[i]}")
            i += 1
        elif re_share_with_isin.match(data[i]):
            # share name and isin in one cell
            current_share = data[i].strip()
            i += 2 if data[i + 1] == "Freiverkehr" else 1  # only if "Freiverkehr" is set as second column
            # print(f"next share is {current_share}")
        elif len(data) - 1 > i and (data[i + 1] == "Freiverkehr" or data[i + 1] == "Regulierter Markt"):
            # old format is ONE row, new one is two rows (cur_i - 1 = ISIN)
            if len(data[i - 1]) == 12:  # if it looks like an ISIN, it is an ISIN.
                current_share = f"{data[i]} ({data[i - 1]})"
            else:
                current_share = f"{data[i]}"
            i += 2  # we skip in any case two elements, as we parsed the share name/isin
            # print(f"next share is {current_share}")
        else:
            # print(f"skipped at {i}: '{data[i]}'")
            i += 1  # skip
        # if i == 117:
        #     print()
    return current_share, local_date, must_replace


def process_pdf_files_task(files_to_process: [str], task_id:int):
    setlocale(LC_NUMERIC, 'en_US')
    # compile regex for text filtering just once per task
    re_time = re.compile("[0-9]{2}:[0-9]{2}:[0-9]{2}")
    # re_isin = re.compile("\([0-9A-Z]{12}\)")
    re_share_with_isin = re.compile("[0-9A-Z\-_,\.\t ]{3,}\([0-9A-Z]{12}\)")
    for c in files_to_process:
        json_file = c.replace('.pdf', '.json')
        if os.path.exists(json_file):
            print(f"JSON file {json_file} exists, skipping PDF read process for source file.")
            continue
        setlocale(LC_NUMERIC, 'en_US')
        last_share_name = ""  # "NAME (SHARE SYMBOL)"
        has2replace = None
        with open(c, "rb") as f:
            shares: {str: [(time, int, float, int)]} = {}
            base_date: date = datetime.now().date()
            print(f"{c} processing started")
            p: PdfFileReader = PdfFileReader(f)
            page_count = p.getNumPages()
            start = timemeasure()
            for i in range(p.getNumPages()):
                last_share_name, base_date, has2replace = parse_kursblatt_page(p.getPage(i), last_share_name, base_date,
                                                                               shares,
                                                                               re_share_with_isin, re_time, has2replace)
                if i == 10:
                    required_for_100: float = timemeasure() - start
                    expected_overall = (required_for_100 * .1) * page_count
                    print(f"expecting {expected_overall - required_for_100} s to go, overall {expected_overall}s")
        print(f"{c} processed in {timemeasure() - start} s")
        with open(f"{json_file}", "w") as f:
            start = timemeasure()
            json.dump(shares, f, cls=JsonDateTimeEncoder)
            print(f"exporting data to {json_file} done in {timemeasure() - start}s")


if __name__ == '__main__':
    # process all pdf files (pdf to json objects) in parallel, 8 processes.
    freeze_support()
    start_all = timemeasure()
    files_to_process = [c for c in glob("../../data/*.pdf")]
    overall_count = len(files_to_process)
    print(f"overall: {overall_count}")
    processes = 8
    chunks_size = int(math.ceil(overall_count / processes))
    p = []
    for i in range(processes):
        bottom = i * chunks_size
        top = (i + 1) * chunks_size if (i + 1) * chunks_size <= overall_count else overall_count
        p.append(
            Process(target=process_pdf_files_task, kwargs=dict(files_to_process=files_to_process[bottom:top], task_id=i)))
    for pro in p:
        pro.start()
    print(f"{processes} processes started, waiting for finish")
    for pro in p:
        if pro is not None:
            pro.join()
