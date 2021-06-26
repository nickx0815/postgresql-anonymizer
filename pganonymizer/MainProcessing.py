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

class MainProcessing(logger):
    endtime = False
    successfullrecords = 0
    successfullfields = 0
    
    def updatesuccessfullrecords(self):
        self.successfullrecords = self.successfullrecords+1
        
    def updatesuccessfullfields(self):
        self.successfullfields = self.successfullfields+1
    
    def __init__(self, totalrecords, schema, table, pg_args):
        self.starttime = time.time()
        self.totalrecords = totalrecords
        self.schema=schema
        self.starttime = time.time()
        self.table=table
        self.totalrecords = totalrecords
        self.pgargs = pg_args
    
    def start(self):
        self.connection = get_connection(self.pgargs)
        self.connection.autocommit = True
        method = self._get_rel_method()
        try:
            getattr(self, method)(self.connection)
        except Exception as exp:
            print(exp)
        finally:
            self.connection.close()
            self.endtime = time.time()
            self.print_info()
    
    def print_info(self):
        runtime = str(datetime.timedelta(seconds=self.endtime-self.starttime))
        main = f"the {self.type} of {self.table} took {runtime}\n"
        additionalrecordsinfo = f"successfull processed {self.successfullrecords} (total records {self.totalrecords})\n"
        additionalfieldsinfo = f"successfull processed {self.successfullfields} fields\n"
        print(main, additionalrecordsinfo, additionalfieldsinfo)
        
        
        