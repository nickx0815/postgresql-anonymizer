"""Helper methods"""


import time
from pganonymizer.utils import get_connection
from pganonymizer.logging import logger
logging_ = logger()

class Job():
    endtime = False
    successfullrecords = 0
    successfullfields = 0

    type_print = {'anonymization': 'Anonymization',
                  'truncate': 'Deletion',
                  'deanonymization': 'Deanonymization'}

    def __init__(self, main_job, totalrecords, data, table, type):
        self.main_job = main_job
        self.logging_ = main_job.logging_
        self.type=type
        self.starttime = time.time()
        self.totalrecords = totalrecords
        self.schema=data
        self.starttime = time.time()
        self.table=table
        self.totalrecords = totalrecords
        self.pg_args = main_job.pg_args

    def updatesuccessfullrecords(self):
        self.successfullrecords = self.successfullrecords+1

    def updatesuccessfullfields(self):
        self.successfullfields = self.successfullfields+1

    def get_connection(self, autocommit=False):
        con = get_connection(self.pg_args)
        con.autocommit = autocommit
        return con

    def set_logger(self, args):
        self.logging_ = logger.get_logger(args, logging_)

    @logging_.RESULTS
    def start(self):
        connection = self.get_connection(autocommit = self._autocommit)
        method = self._get_run_method()
        try:
            getattr(self, method)(connection)
        except Exception as exp:
            #todo use logger
            print(exp)
        finally:
            connection.close()
            self.endtime = time.time()

    def _get_run_method(self):
        raise Exception("needs to be implemented!")

    def type_to_method_mapper(self, type):
        return self.type_print[self.type]
    