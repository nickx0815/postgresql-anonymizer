"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import threading
import sys
from queue import Queue

import yaml

from pganonymizer.constants import constants 
from pganonymizer.providers import PROVIDERS
from pganonymizer.utils import get_connection, get_pg_args, create_basic_tables
from pganonymizer.logging import logger
from pganonymizer.analyse import run_analyse
logging_ = logger()

class BaseJobClass():
    jobs = Queue()
    
    def __init__(self, args):
        self.args = args
        self.set_logger()
        self.set_pg_args()
        self.set_schema()
        
    def get_pg_args(self):
        return self.pg_args
    
    def set_pg_args(self):
        self.pg_args = get_pg_args(self.args)
    
    def set_args(self, args):
        self.args = args
        
    def get_args(self):
        return self.args
    
    def get_jobs(self):
        return self.jobs
    
    def add_job(self, job):
        self.jobs.put(job)
    
    def get_schema(self):
        return self.schema
    
    @logging_.SET_SCHEMA
    def set_schema(self):
        forced_schema_path = self.args.force_path_schema
        if forced_schema_path:
            path = forced_schema_path
        else:
            path = f"{constants.PATH_SCHEMA_FILES}{self.args.schema}"
        try:
            schema = yaml.load(open(path), Loader=yaml.FullLoader)
        except:
            schema = yaml.load(open(path))
        finally:
            self.schema = schema
    
    
    def set_logger(self):
        self.logging_ = logging_.set_log_level(self.args)
    
    def is_starting_up_error(self, oe):
        if constants.STARTINGUPERROR in oe.args[0]:
            return True
        return False
    
    @logging_.TEST_CONNECTION
    def test_connection(self):
        while True:
            try:
                self.get_connection()
                return True
            except Exception as exc:
                if self.is_starting_up_error(exc):
                    continue
                raise exc
    
    @logging_.PRINT_PROCESSING_TIME    
    def start_processing(self):
        """Main method"""
        self.update_queue()
        self.test_connection()
        create_basic_tables(self.get_connection())
        self.start()
        run_analyse(self.get_connection(autocommit=True), self.args.dbname)
    
    def get_connection(self, autocommit=False):
        con = get_connection(self.pg_args)
        con.autocommit = autocommit
        return con
    
    def start(self):
        if self.args.threading in ['False','false']:
            self.run_job(self.jobs)  
        else:
            number_threads = self.get_thread_number()
            for i in range(number_threads):
                worker = threading.Thread(target=self.run_job, args=(self.jobs,))
                worker.start()
            self.jobs.join()
    
    def list_provider_classes(self):
        """List all available provider classes."""
        #print('Available provider classes:\n')
        for provider_cls in PROVIDERS:
            #todo use logging_
            print('{:<10} {}'.format(provider_cls.id, provider_cls.__doc__))
    
    def run_job(self, jobs):
        while not jobs.empty():
            current_job = jobs.get()
            current_job.start()
            jobs.task_done()
        
    def _get_queue_size(self):
        return self.jobs.qsize()
    
    @logging_.NUMBER_THREAD
    def get_thread_number(self):
        queue_size = self._get_queue_size()
        thread = constants.DEFAULT_NUMBER_THREADS
        force_thread_number = self.args.force_thread_number
        if force_thread_number:
            return int(force_thread_number)
        number_threads = queue_size if queue_size < thread else thread
        return number_threads
    
    