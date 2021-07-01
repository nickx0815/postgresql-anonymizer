"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import threading
import logging
from queue import Queue

import yaml

from pganonymizer.constants import constants 
from pganonymizer.providers import PROVIDERS
from pganonymizer.utils import create_database_dump, get_connection, get_pg_args, create_basic_tables
from pganonymizer.logging import logger


class BaseMain():
    logger = logger()
    jobs = Queue()
    schema = False
    pg_args = False
    migration = False
    
    def __init__(self, args):
        self.args = args
        self.logger.setLogLevel(args)
        self.pg_args = get_pg_args(args)
        self.test_connection()
        create_basic_tables(get_connection(self.pg_args))
        self.get_schema(args)
        self.update_queue()
    
    def set_migration(self, args):
        migration = args.get('migration')
        self.migration = migration
    
    def isStartingUpError(self, oe):
        if constants.STARTINGUPERROR in oe.args[0]:
            return True
        return False
    
    @logger.TEST_CONNECTION
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

        self.start(self.args)
        dump_path = self.args.dump_file
        if dump_path:
            create_database_dump(self.pg_args)
    
    def start(self, args):
        if args.threading == 'False':
            self.start_thread(self.jobs)  
        else:
            number_threads = self.get_thread_number()
            for i in range(number_threads):
                worker = threading.Thread(target=self.start_thread, args=(self.jobs,))
                worker.start()
            self.jobs.join()
    
    @logger.GET_SCHEMA
    def get_schema(self, args):
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
            #todo use logger
            print('{:<10} {}'.format(provider_cls.id, provider_cls.__doc__))
    
    def start_thread(self, q):
        while not q.empty():
            data = q.get()
            self._runSpecificTask(data)
            q.task_done()
    
    @logger.THREAD_STARTED
    def _runSpecificTask(self, job):
        job.start()
    
    @logger.NUMBER_THREAD
    def get_thread_number(self):
        queue_size = self.jobs.qsize()
        thread = getattr(constants, self.THREAD)
        force_thread_number = self.args.get('force_thread_number')
        if force_thread_number and force_thread_number > thread:
            thread=force_thread_number
        number_threads = queue_size if queue_size < thread else thread
        return number_threads
    
    