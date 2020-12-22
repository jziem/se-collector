# ingest all json data to database
import json
import math
from datetime import datetime
from glob import glob
from json.decoder import JSONDecoder
from multiprocessing.context import Process
from multiprocessing.spawn import freeze_support
from time import time

from se_collector.db.lsx_db_model import ShareTransaction, Share, share_transaction_bulk_upsert, setup_database

class JsonDateTimeDecoder(JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.try_datetime, *args, **kwargs)

    @staticmethod
    def try_datetime(d):
        ret = {}
        for key, value in d.items():
            # key is name+ISIN
            # value is an array of value-arrays
            if isinstance(value, list):
                new_arr = []
                for v in value:
                    # fromisoformat doesnt exist at 3.6
                    new_arr.append([datetime.strptime(v[0],'%Y-%m-%dT%H:%M:%S'), v[1], v[2], v[3]])
                value = new_arr
            ret[key] = value
        return ret

def load_json_files_to_database(arr: [str], process_id: int):
    """
    Process an ingest job (thread/process safe)
    :param arr: array of absolute path to json data files
    :param process_id: a process number to log data regarding to task/process
    :return:
    """
    count_of_entries = 0
    for i in range(len(arr)):
        s = time()
        bulk_ta: [ShareTransaction] = []
        with open(arr[i]) as f:
            result = json.load(f, cls=JsonDateTimeDecoder)
            for k in result.keys():
                # upsert of a share definition
                x = Share.get_or_create(share_full_name=k.strip(), share_name=k[:-14].strip(),
                                        share_isin=k[-13:-1].strip())
                seq = 0
                for entry in result.get(k):
                    count_of_entries += 1
                    # fill the bulk list with transactions
                    bulk_ta.append(ShareTransaction(share_id=x.id,
                                                    ts=entry[0],
                                                    sequno=seq,
                                                    volume=entry[1],
                                                    value=entry[2],
                                                    order_type="S" if entry[3] == 0 else "B"))
                    seq += 1
        # now insert values in bulk:
        share_transaction_bulk_upsert(bulk_ta)
        print(f"[{process_id}] procssed {i + 1}/{len(arr)} {arr[i]} in {time() - s}")
    print(f"[{process_id}] processing job did count of entries overall: {count_of_entries}")


if __name__ == '__main__':
    # This module is able to ingest one year of data (252 files, 614MB, 15.8 billion trade transactions) in parallel
    freeze_support()
    setup_database()
    files_to_process = [c for c in glob("../../data/*.json")]
    overall_count = len(files_to_process)
    print(f"overall: {overall_count}")
    processes = 8
    chunks_size = int(math.ceil(overall_count / processes))
    p = []
    for i in range(8):
        bottom = i * chunks_size
        top = (i + 1) * chunks_size if (i + 1) * chunks_size <= overall_count else overall_count
        p.append(
            Process(target=load_json_files_to_database, kwargs=dict(arr=files_to_process[bottom:top], process_id=i)))
    for pro in p:
        pro.start()
    print("processes started, waiting for finish")
    for pro in p:
        if pro is not None:
            pro.join()
