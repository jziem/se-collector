import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
import env_settings as cfg

# TODO: uncomment for full blown sql statement logging
# class LoggingConnection(psycopg2.extras.LoggingConnection):
#
#     def __init__(self, *args, **kwargs):
#         super(LoggingConnection, self).__init__(*args, **kwargs)
#         self.initialize(logging.getLogger(__name__))

class ConnectionManager:

    def __init__(self):
        logging.info("initialization of connection object")
        self.engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}/{3}'.format(
            cfg.DBUSER, cfg.DBPWD, cfg.DBHOST, cfg.DBNAME), pool_size=2, max_overflow=4, pool_recycle=1,
            connect_args={'options': '-csearch_path={}'.format(cfg.DBSCHEMA)
                          # ,'connection_factory': LoggingConnection
                          },
            # echo=True
        )

    def open(self):
        logging.debug("opening database connection")
        connection = self.engine.connect()
        return ConnectionScope(connection)


class ConnectionScope:

    def __init__(self, connection: Connection):
        self.connection = connection

    def __enter__(self):
        logging.debug("starting transaction")
        self.transaction = self.connection.begin()
        return self.connection

    def __exit__(self, t, v, tb):
        if tb is None:
            # No exception, so commit
            logging.debug("committing changes")
            self.transaction.commit()
        else:
            # Exception occurred, so rollback.
            logging.warning("rollback of changes")
            self.transaction.rollback()
        logging.debug("closing database connection")
        # Put connection back to pool
        self.connection.close()


connection_manager = ConnectionManager()


def cleanup():
    logging.info("disposing connection")
    connection_manager.engine.dispose()


import atexit

atexit.register(cleanup)
