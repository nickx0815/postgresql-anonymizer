"""Helper methods"""

from __future__ import absolute_import

import csv
import json
import logging
import re
import subprocess
import time
import datetime

import psycopg2
import psycopg2.extras
from progress.bar import IncrementalBar
from psycopg2.errors import BadCopyFileFormat, InvalidTextRepresentation
from six import StringIO

from pganonymizer.constants import constants
from pganonymizer.exceptions import BadDataFormat
from pganonymizer.providers import get_provider
from pganonymizer.utils import _get_ids_sql_format, _, get_table_count, build_sql_select, update_fields_history, get_connection
from pganonymizer.logging import logger

class MainProcessing():
    logger = logger
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
    
    def __init__(self, totalrecords, schema, table, pg_args, logger, type):
        self.type=type
        self.logger=logger
        self.starttime = time.time()
        self.totalrecords = totalrecords
        self.schema=schema
        self.starttime = time.time()
        self.table=table
        self.totalrecords = totalrecords
        self.pgargs = pg_args
    
    @logger.RESULTS
    def start(self):
        self.connection = get_connection(self.pgargs)
        self.connection.autocommit = True
        method = self._get_rel_method()
        try:
            getattr(self, method)(self.connection)
        except Exception as exp:
            #todo use logger
            print(exp)
        finally:
            self.connection.close()
            self.endtime = time.time()
    
    def _get_rel_method(self):
        raise Exception("needs to be implemented!")
    
    def type_to_method_mapper(self, type):
        return self.typemethodmapper.get(type, 'Deanonymization')
    
        