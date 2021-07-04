"""Helper methods"""

from __future__ import absolute_import

import csv
import json
import logging
import re
import subprocess
import time
import datetime
from pganonymizer.utils import get_connection
from pganonymizer.logging import logger
logging_ = logger()

class MainProcessing():
    endtime = False
    successfullrecords = 0
    successfullfields = 0
    
    typemethodmapper = {'tables': 'Anonymization',
                        'truncate': 'Deletion',
                        'deanon': 'Deanonymization'}
    
    def updatesuccessfullrecords(self):
        self.successfullrecords = self.successfullrecords+1
        
    def updatesuccessfullfields(self):
        self.successfullfields = self.successfullfields+1
    
    def __init__(self, totalrecords, schema, table, pg_args, type ,logger):
        self.logging_ = logger
        self.type=type
        self.starttime = time.time()
        self.totalrecords = totalrecords
        self.schema=schema
        self.starttime = time.time()
        self.table=table
        self.totalrecords = totalrecords
        self.pgargs = pg_args
    
    def get_connection(self, autocommit=False):
        con = get_connection(self.pg_args)
        con.autocommit = autocommit
        return con
    
    def set_logger(self, args):
        self.logging_ = logger.get_logger(args, logging_)
    
    @logging_.RESULTS
    def start(self):
        connection = get_connection(autocommit = True)
        method = self._get_rel_method()
        try:
            getattr(self, method)(connection)
        except Exception as exp:
            #todo use logger
            print(exp)
        finally:
            connection.close()
            self.endtime = time.time()
    
    def _get_rel_method(self):
        raise Exception("needs to be implemented!")
    
    def type_to_method_mapper(self, type):
        return self.typemethodmapper.get(type, 'Deanonymization')
    
        