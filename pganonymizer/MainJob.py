"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import threading
import sys
from queue import Queue

import yaml

from pganonymizer.constants import constants 
from pganonymizer.providers import PROVIDERS
from pganonymizer.utils import create_database_dump, get_connection, get_pg_args, create_basic_tables
from pganonymizer.logging import logger
logging_ = logger()

class BaseJobClass():
    jobs = Queue()
    schema = False
    pg_args = False
    migration = False
    
    def __init__(self, args):
        self.logging_ = self.get_logger(args)
        if args.list_providers:
            self.list_provider_classes()
            sys.exit(0)
        self.pg_args = get_pg_args(args)
        self.args = args
        self.set_schema(args)
        
    
    def get_logger(self, args):
        logging_.setLogLevel(args)
        return logging_
    
    def is_starting_up_error(self, oe):
        if constants.STARTINGUPERROR in oe.args[0]:
            return True
        return False
    
    @logging_.TEST_CONNECTION
    def test_connection(self):
        args = self.pg_args
        while True:
            try:
                self.get_connection(args)
                return True
            except Exception as exc:
                if self.is_starting_up_error(exc):
                    continue
                raise exc
        
    def start_processing(self):
        """Main method"""
        args = self.args
        self.update_queue()
        self.test_connection()
        create_basic_tables(self.get_connection(self.pg_args))
        self.start()
        if args.dump_file:
            create_database_dump(self.pg_args)
    
    def get_connection(self, args):
        return get_connection(args)
    
    def start(self):
        args = self.args
        if args.threading in ['False','false']:
            self.run_job(self.jobs)  
        else:
            number_threads = self.get_thread_number()
            for i in range(number_threads):
                worker = threading.Thread(target=self.run_job, args=(self.jobs,))
                worker.start()
            self.jobs.join()
    
    @logging_.SET_SCHEMA
    def set_schema(self, args):
        forced_schema_path = args.force_path_schema
        if forced_schema_path:
            path = forced_schema_path
        else:
            path = f"{constants.PATH_SCHEMA_FILES}{args.schema}"
        try:
            schema = yaml.load(open(path), Loader=yaml.FullLoader)
        except:
            schema = yaml.load(open(path))
        finally:
            self.schema = schema
        
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
        thread = getattr(constants, self.THREAD)
        force_thread_number = self.args.force_thread_number
        if force_thread_number:
            return int(force_thread_number)
        number_threads = queue_size if queue_size < thread else thread
        return number_threads
    
    