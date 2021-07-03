"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import threading
from queue import Queue

import yaml

from pganonymizer.constants import constants 
from pganonymizer.providers import PROVIDERS
from pganonymizer.utils import create_database_dump, get_connection, get_pg_args, create_basic_tables
from pganonymizer.logging import logger

logging = logging()
class BaseMain():

    jobs = Queue()
    schema = False
    pg_args = False
    migration = False
    
    def __init__(self, args):
        self.args = args
        logging.setLogLevel(args)
        self.pg_args = get_pg_args(args)
        self.get_schema()
    
#     def set_migration(self, args):
#         migration = args.get('migration')
#         self.migration = migration
    
    def isStartingUpError(self, oe):
        if constants.STARTINGUPERROR in oe.args[0]:
            return True
        return False
    
    @logging.TEST_CONNECTION
    def test_connection(self):
        args = self.pg_args
        while True:
            try:
                get_connection(args)
                return True
            except Exception as exc:
                if self.isStartingUpError(exc):
                    continue
                raise exc
        
    def startprocessing(self):
        """Main method"""
        args = self.args
        self.update_queue()
        self.test_connection()
        create_basic_tables(get_connection(self.pg_args))
        self.start()
        if args.dump_file:
            create_database_dump(self.pg_args)
    
    def start(self):
        args = self.args
        if args.threading in ['False','false']:
            self.start_thread(self.jobs)  
        else:
            number_threads = self.get_thread_number()
            for i in range(number_threads):
                worker = threading.Thread(target=self.start_thread, args=(self.jobs,))
                worker.start()
            self.jobs.join()
    
    @logging.GET_SCHEMA
    def get_schema(self):
        args = self.args
        if args.force_path_schema:
            path=args.force_path_schema
        else:
            path = f"{constants.PATH_SCHEMA_FILES}{args.schema}"
        try:
            schema = yaml.load(open(path), Loader=yaml.FullLoader)
        except:
            schema = yaml.load(open(path))
        self.schema = schema
        
    def list_provider_classes(self):
        """List all available provider classes."""
        #print('Available provider classes:\n')
        for provider_cls in PROVIDERS:
            #todo use logging
            print('{:<10} {}'.format(provider_cls.id, provider_cls.__doc__))
    
    def start_thread(self, q):
        while not q.empty():
            data = q.get()
            self._runSpecificTask(data)
            q.task_done()
    
    @logging.THREAD_STARTED
    def _runSpecificTask(self, job):
        job.start()
        
    def _get_qsize(self):
        return self.jobs.qsize()
    
    @logging.NUMBER_THREAD
    def get_thread_number(self):
        queue_size = self._get_qsize()
        thread = getattr(constants, self.THREAD)
        force_thread_number = self.args.force_thread_number
        if force_thread_number:
            return int(force_thread_number)
        number_threads = queue_size if queue_size < thread else thread
        return number_threads
    
    