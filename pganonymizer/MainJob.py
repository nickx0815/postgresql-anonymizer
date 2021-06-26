"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import threading
import logging
from queue import Queue

import yaml

from pganonymizer.constants import constants 
from pganonymizer.providers import PROVIDERS
from pganonymizer.utils import create_database_dump, get_connection, get_pg_args
from pganonymizer.logging import logger
logger = logger()

class BaseMain():
    jobs = Queue()
    schema = False
    pg_args = False
    
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
                break
            except Exception as exc:
                if self.isStartingUpError(exc):
                    continue
                raise exc
        
    def startprocessing(self, args_):
        """Main method"""
        # own connection per schema batch...
        args_ = self._get_run_data(args_)
        self.test_connection()
        self.setLogLevel(args_)
        self.get_schema(args_)
        self.update_queue()
        if args_.threading == 'False':
            self.start_thread(self.jobs, args_)  
        else:
            number_threads = self.get_thread_number()
            #print(f"Number of threads started: {number_threads}")
            for i in range(number_threads):
                worker = threading.Thread(target=self.start_thread, args=(self.jobs,args_))
                worker.start()
            print("waiting for queue to complete tasks")
            self.jobs.join()
        print("all done")
        dump_path = args_.dump_file
        if dump_path:
            create_database_dump(self.pg_args)
    
    def get_schema(self, args):
        if args.force_path:
            path=args.force_path
        else:
            path = f"{constants.PATH_SCHEMA_FILES}{args.schema}"
        #path = "./schema/anonschema.yaml"
        try:
            schema = yaml.load(open(path), Loader=yaml.FullLoader)
        except:
            schema = yaml.load(open(path))
        self.schema = schema
        
    def list_provider_classes(self):
        """List all available provider classes."""
        print('Available provider classes:\n')
        for provider_cls in PROVIDERS:
            print('{:<10} {}'.format(provider_cls.id, provider_cls.__doc__))
    
    def get_args(self, parseargs=True):
        parser = argparse.ArgumentParser(description='Anonymize data of a PostgreSQL database')
        parser.add_argument('--schema', help='A YAML schema file that contains the anonymization rules',
                            default=constants.DEFAULT_SCHEMA_FILE)
        parser.add_argument('--dbname', help='Name of the database')
        parser.add_argument('--user', help='Name of the database user')
        parser.add_argument('--password', default='', help='Password for the database user')
        parser.add_argument('--host', help='Database hostname', default='localhost')
        parser.add_argument('--port', help='Port of the database', default='5432')
        parser.add_argument('--dry-run', action='store_true', help='Don\'t commit changes made on the database',
                            default=False)
        if parseargs:
            args = parser.parse_args()
            return args
        return parser
    
    def start_thread(self, q, args):
        while not q.empty():
            data = q.get()
            self._runSpecificTask(args, data)
            q.task_done()
    
    def _get_run_data(self, args):
        if not args:
            args = self.get_args()
        self.pg_args = get_pg_args(args)
        return args
    
    def get_thread_number(self):
        queue_size = self.jobs.qsize()
        thread = getattr(constants, self.THREAD)
        number_threads = queue_size if queue_size < thread else thread
        return number_threads