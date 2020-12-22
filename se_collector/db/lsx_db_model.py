import math
from datetime import datetime
from time import time

from se_collector.db import db_handler as db

SHARE_TABLE = "shares"
SHARE_TRANSACTION_TABLE = "shares_transactions"


class Share:
    def __init__(self, id: int, share_full_name: str, share_name: str, share_isin: str):
        self.id = id
        self.share_full_name = share_full_name
        self.share_name = share_name
        self.share_isin = share_isin

    @staticmethod
    def get_or_create(share_full_name: str, share_name: str, share_isin: str):
        with db.connection_manager.open() as con:
            c = con.connection.cursor()
            upsert_stmt = f"""insert into {SHARE_TABLE} (share_full_name, share_name, share_isin)
                            values (%s,%s,%s) on conflict (share_full_name) do update
                            set share_full_name=excluded.share_full_name,
                            share_name=excluded.share_name, share_isin=excluded.share_isin
                            returning id;"""
            c.execute(upsert_stmt, [share_full_name, share_name, share_isin])
            rows = c.fetchall()
            if len(rows) == 1:
                return Share(id=rows[0][0], share_full_name=share_full_name, share_name=share_name,
                             share_isin=share_isin)
            else:
                raise Exception("Unexpected state, no id returned.")

    @staticmethod
    def get_table_create_statement():
        return f"""create table if not exists {SHARE_TABLE}
                        (id serial,
                         share_full_name character varying(100) NOT NULL,
                         share_name character varying(100) NOT NULL,
                         share_isin character varying(12) NOT NULL,
                         PRIMARY KEY (id),
                         CONSTRAINT uc_{SHARE_TABLE} UNIQUE (share_full_name)
                         );"""


class ShareTransaction:
    def __init__(self, ts: datetime, share_id: int, sequno: int, volume: int, value: float, order_type: str):
        self.ts: datetime = ts
        self.share_id = share_id
        self.sequno = sequno
        self.volume = volume
        self.value = value
        self.order_type = order_type

    @staticmethod
    def get_table_create_statement():
        return f"""create table if not exists {SHARE_TRANSACTION_TABLE}
                        (ts timestamp without time zone NOT NULL,
                         share_id integer NOT NULL,
                         sequno integer NOT NULL,
                         volume integer NOT NULL,
                         value FLOAT NOT NULL,
                         order_type CHAR(1) NOT NULL,
                         PRIMARY KEY (ts, share_id, sequno));"""


def share_transaction_bulk_upsert(huge_data: [ShareTransaction]):
    # now first try to fetch the element (cheaper than upsert)
    with db.connection_manager.open() as con:
        c = con.connection.cursor()
        sql_param_field_names = ['ts', 'share_id', 'sequno', 'volume', 'value', 'order_type']
        joined_fields = ','.join(sql_param_field_names)
        conflicting_fields = "ts, share_id, sequno"  # all conflict keys
        update_collisions = ', '.join(['{}=excluded.{}'.format(f, f) for f in sql_param_field_names])
        values_template = f"({', '.join(['%s'] * len(sql_param_field_names))})"
        STEP_SIZE = 100000
        for i in range(math.ceil(len(huge_data) / STEP_SIZE)):
            st = time()
            # sd => sliced_data
            sd = huge_data[i * STEP_SIZE:i * STEP_SIZE + STEP_SIZE]
            sd_len = len(sd)  # len of sliced data
            values_stm = ', '.join([values_template] * sd_len)
            insert_sql = f"insert into {SHARE_TRANSACTION_TABLE} ({joined_fields}) " \
                         f"values {values_stm} " \
                         f"on conflict ({conflicting_fields}) do update set {update_collisions}"
            insert_data = []
            for x in range(sd_len):
                x: ShareTransaction
                insert_data.extend(
                    [sd[x].ts, sd[x].share_id, sd[x].sequno, sd[x].volume, sd[x].value, sd[x].order_type])
            c.execute(insert_sql, insert_data)  # insert/update the data
            con.connection.commit()
            print(
                f"committed chunk ({sd_len} transactions) in {time() - st}s {i + 1}/{math.ceil(len(huge_data) / STEP_SIZE)} len={sd_len}")


def setup_database():
    with db.connection_manager.open() as con:
        c = con.connection.cursor()
        c.execute(Share.get_table_create_statement())
        c.execute(ShareTransaction.get_table_create_statement())
