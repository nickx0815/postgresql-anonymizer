"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import logging
import sys
import time
import psycopg2
from time import sleep
import threading, time
from queue import Queue

import yaml

from pganonymizer.constants import DATABASE_ARGS, DEFAULT_SCHEMA_FILE, NUMBER_MAX_THREADS
from pganonymizer.providers import PROVIDERS
from pganonymizer.utils import anonymize_tables, create_database_dump, get_connection, truncate_tables, build_sql_select
from pganonymizer.revert import run_revert, _get_ids_sql_format

def get_pg_args(args):
        """
        Map all commandline arguments with database keys.
    
        :param argparse.Namespace args: The commandline arguments
        :return: A dictionary with database arguments
        :rtype: dict
        """
        return ({name: value for name, value in
                 zip(DATABASE_ARGS, (args.dbname, args.user, args.password, args.host, args.port))})

class BaseMain():
    def __init__(self):
        self.jobs = Queue()
        
    def main_anonymize(self, args_, opt_args):
        """Main method"""
        # own connection per schema batch...
        pg_args, args_ = self._get_run_data(args_)
    
        loglevel = logging.WARNING
        if args_.verbose:
            loglevel = logging.DEBUG
        logging.basicConfig(format='%(levelname)s: %(message)s', level=loglevel)
    
        if args_.list_providers:
            self.list_provider_classes()
            sys.exit(0)
        opt_args['pg_args']=pg_args
        self.update_queue(args_, opt_args)
        if opt_args.get('threading'):
            number_threads = self.get_thread_number()
            print("Number of threads started: {number}".format(number=number_threads))
            for i in range(number_threads):
                worker = threading.Thread(target=self.start_thread, args=(self.jobs,args_, pg_args))
                worker.start()
            
            print("waiting for queue to complete tasks")
            self.jobs.join()
        else:
            self.start_thread(self.jobs, args_, pg_args)   
        print("all done")
        
    def list_provider_classes(self):
        """List all available provider classes."""
        print('Available provider classes:\n')
        for provider_cls in PROVIDERS:
            print('{:<10} {}'.format(provider_cls.id, provider_cls.__doc__))
    
    def get_args(self):
        parser = argparse.ArgumentParser(description='Anonymize data of a PostgreSQL database')
        parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity')
        parser.add_argument('-l', '--list-providers', action='store_true', help='Show a list of all available providers',
                            default=False)
        parser.add_argument('--schema', help='A YAML schema file that contains the anonymization rules',
                            default=DEFAULT_SCHEMA_FILE)
        parser.add_argument('--dbname', help='Name of the database')
        parser.add_argument('--user', help='Name of the database user')
        parser.add_argument('--password', default='', help='Password for the database user')
        parser.add_argument('--host', help='Database hostname', default='localhost')
        parser.add_argument('--port', help='Port of the database', default='5432')
        parser.add_argument('--dry-run', action='store_true', help='Don\'t commit changes made on the database',
                            default=False)
        parser.add_argument('--dump-file', help='Create a database dump file with the given name')
    
        args = parser.parse_args()
        return args
    
    def _get_run_data(self, args):
        if not args:
            args = self.get_args()
        pg_args = get_pg_args(args)
        return pg_args, args
    
    def get_thread_number(self):
        return 0
    
class AnonymizationMain(BaseMain):
    def update_queue(self, args, opt_args):
        schema = yaml.load(open(args.schema), Loader=yaml.FullLoader)
        self.get_schema_batches(schema, opt_args)
    
    def get_schema_batches(self, schema, opt_args):
        #todo konfigurierbar
        #search wird nicht übernommen
        connection = get_connection(opt_args['pg_args'])
        for type_, type_attributes in schema.items():
            for table in type_attributes:
                if type(table) == str:
                    self.jobs.put({type_: [table]})
                else:
                    for table_key, table_attributes in table.items():
                        cursor = build_sql_select(connection, table_key, table_attributes['search'], select="id")
                        while True:
                            list = []
                            records = cursor.fetchmany(size=1000)
                            if not records:
                                break
                            for row in records:
                                list.append(row.get('id'))
                            cur = table_attributes       
                            cur['search'].append("id in ("+_get_ids_sql_format(list)+")")
                            self.jobs.put({type_: [{table_key:cur}]})
        
    def get_thread_number(self):
        queue_size = self.jobs.qsize()
        number_threads = queue_size if queue_size < NUMBER_MAX_THREADS else NUMBER_MAX_THREADS
        return number_threads
    
    def start_thread(self, q, args, pg_args):
        while not q.empty():
            start_time = time.time()
            schema = q.get()
            connection = get_connection(pg_args)
#             try:
                #todo implement truncate functionality, not working right now
                #truncate_tables(connection, schema_batch.get('truncate', []))
            try:
                print("starting thread "+str(self))
                anonymize_tables(connection, schema.get('tables', []), verbose=args.verbose)
                if not args.dry_run:
                    connection.commit()
                end_time = time.time()
                logging.info('Anonymization took {:.2f}s'.format(end_time - start_time))
            except Exception as ex:
                logging.info(ex)
            connection.close()
            q.task_done()

class DeAnonymizationMain(BaseMain):
    def update_queue(self,args_, opt_args):
        where_clause = " 1=1"
        NUMBER_FIELD_PER_THREAD = 1
        pg_args, args_ = self._get_run_data(args_)
        connection = get_connection(pg_args)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if opt_args.get('ids'):
            where_clause = "id in {ids}".format(ids=_get_ids_sql_format(opt_args.get('ids')))
        cursor.execute(("select id from migrated_fields where {where}").format(where=where_clause))
        while True:
            job_ids = []
            records = cursor.fetchmany(size=NUMBER_FIELD_PER_THREAD)
            if not records:
                break
            for row in records:
                job_ids.append(row['id'])
            self.jobs.put(job_ids)
        cursor.close()

        
    def get_thread_number(self):
        return NUMBER_MAX_THREADS
    
    def start_thread(self, q, _args, pg_args):
        while not q.empty():
            start_time = time.time()
            data = q.get()
            connection = get_connection(pg_args)
            #todo implement truncate functionality, not working right now
            try:
                run_revert(connection, _args, data)
    #                 if not args.dry_run:
    #                     connection.commit()
                end_time = time.time()
                logging.info('DEAnonymization took {:.2f}s'.format(end_time - start_time))
            except Exception as ex:
                logging.info(ex)
            connection.close()
            q.task_done()

def main():
    #todo needs to be implemented, run the script via command line. 
    # the args need to be analysed here, if anonymization or
    # deanonymization is running
    return





#     if args.dump_file:
#         create_database_dump(args.dump_file, pg_args)
    

if __name__ == '__main__':
    main()
