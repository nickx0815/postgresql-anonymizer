"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import logging
import sys
import copy
import time
import datetime
import psycopg2
from time import sleep
import threading, time
from queue import Queue

import yaml

from pganonymizer.constants import constants
from pganonymizer.providers import PROVIDERS
from pganonymizer.utils import anonymize_tables, create_database_dump, get_connection, truncate_tables, build_sql_select
from pganonymizer.revert import run_revert, _get_ids_sql_format, get_mapped_field_data

def get_pg_args(args):
        """
        Map all commandline arguments with database keys.
    
        :param argparse.Namespace args: The commandline arguments
        :return: A dictionary with database arguments
        :rtype: dict
        """
        return ({name: value for name, value in
                 zip(constants.DATABASE_ARGS, (args.dbname, args.user, args.password, args.host, args.port))})

class BaseMain():
    jobs = Queue()
    number_rec = {}
        
    def startProcessing(self, args_, opt_args):
        """Main method"""
        # own connection per schema batch...
        pg_args, args_ = self._get_run_data(args_)
        opt_args['pg_args']=pg_args
        schema = self.get_schema(args_)
        self.update_queue(schema, opt_args)
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
    
    def get_schema(self, args):
        try:
            schema = yaml.load(open(args.schema), Loader=yaml.FullLoader)
        except:
            schema = yaml.load(open(args.schema))
        return schema
        
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
    
    def start_thread(self, q, args, pg_args):
        while not q.empty():
            #table_start_time = time.time()
            data = q.get()
            connection = get_connection(pg_args)
                #todo implement truncate functionality, not working right now
                #truncate_tables(connection, schema_batch.get('truncate', []))
            self._runSpecificTask(connection, args, data)
            if not args.dry_run:
                connection.commit()
            connection.close()
            q.task_done()
    
    def _get_run_data(self, args):
        if not args:
            args = self.get_args()
        pg_args = get_pg_args(args)
        return pg_args, args
    
    def get_thread_number(self):
        queue_size = self.jobs.qsize()
        number_threads = queue_size if queue_size < constants.NUMBER_MAX_THREADS else constants.NUMBER_MAX_THREADS
        return number_threads
    
class AnonymizationMain(BaseMain):
    
    def get_args(self):
        parser =  BaseMain.get_args(self, parseArgs=False)
        parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity')
        parser.add_argument('-l', '--list-providers', action='store_true', help='Show a list of all available providers',
                            default=False)
        parser.add_argument('--dump-file', help='Create a database dump file with the given name')
        args = parser.parse_args()
        return args
    
    
    def startProcessing(self, args_, opt_args):
        loglevel = logging.WARNING
        if args_.verbose:
            loglevel = logging.DEBUG
        logging.basicConfig(format='%(levelname)s: %(message)s', level=loglevel)
        if args_.list_providers:
            self.list_provider_classes()
            sys.exit(0)
        BaseMain.startProcessing(self, args_, opt_args)
    
    def update_queue(self, schema, opt_args):
        #todo konfigurierbar
        #search wird nicht übernommen
        connection = get_connection(opt_args['pg_args'])
        for type_, type_attributes in schema.items():
            for table in type_attributes:
                if type(table) == str:
                    self.jobs.put({type_: [table]})
                else:
                    for table_key, table_attributes in table.items():
                        number = 0
                        cursor = build_sql_select(connection, table_key, table_attributes.get('search', False), select="id")
                        while True:
                            list = []
                            records = cursor.fetchmany(size=constants.ANON_NUMBER_FIELD_PER_THREAD)
                            number = number + len(records)
                            
                            if not records:
                                break
                            for row in records:
                                list.append(row.get('id'))
                            cur = copy.deepcopy(table_attributes)    
                            search_list = cur.get('search', [])
                            search_list.append("id in "+_get_ids_sql_format(list))
                            cur['search'] = search_list
                            self.jobs.put({type_: [{table_key:cur}]})
                        self.number_rec[table_key] = (number, 0, time.time())
                        
    def print_info(self, table, total, anonymized, percent_anonymized):
        print("Table {table} is {percent} % anonymized".format(table=table,
                                                                percent="{:.2f}".format(percent_anonymized*100)))
        total_anonymized = percent_anonymized*total
        print("Total Records anonymized {total}".format(total=total_anonymized-(total_anonymized%1)))
        if percent_anonymized == 1:
            runtime = time.time()-self.number_rec[table][2]
            time_ = str(datetime.timedelta(seconds=runtime))
            print("Table {table} is anonymized".format(table=table))
            print("Anonymization of {table} took {time}".format(table=table,
                                                                time = time_))
        self.number_rec[table] = (total, anonymized, self.number_rec[table][2])
    
    def _runSpecificTask(self, con, args, schema):
        try:
            res, table = anonymize_tables(con, schema.get('tables', []), verbose=args.verbose)
            total_size = self.number_rec[table][0]
            number_anonymized = self.number_rec[table][1]+res
            percent_anonymized = number_anonymized/total_size
            self.print_info(table, total_size, number_anonymized, percent_anonymized)
        except Exception as ex:
            logging.info(ex)

class DeAnonymizationMain(BaseMain):
    def update_queue(self,schema, opt_args):
        connection = get_connection(opt_args['pg_args'])
        #todo umbauen, dass ein job jeweils alle migrated_fields eines records beinhaltet. 
        #todo weitere deanon methoden umbaunen, sodass alle felder mit einem update deanonymsiert werden
        for table, fields in schema.items():
            for field in fields:
                cursor = build_sql_select(connection, "migrated_data", 
                                                                    ["model_id = '{model_id}'".format(model_id=table),
                                                                    "field_id = '{field_id}'".format(field_id=field)],
                                                                    select="record_id, value")
                while True:
                    list = []
                    records = cursor.fetchmany(size=constants.DEANON_NUMBER_FIELD_PER_THREAD)
                    if not records:
                        break
                    for rec in records:
                        list.append((rec.get('record_id'), rec.get('value')))
                    self.jobs.put({table: (field, list)})

    def _runSpecificTask(self, con, args, data):
        try:
            start_time = time.time()
            run_revert(con, args, data)
            end_time = time.time()
            logging.info('DEAnonymization took {:.2f}s'.format(end_time - start_time))
        except Exception as ex:
            logging.info(ex)

def main():
    #todo needs to be implemented, run the script via command line. 
    # the args need to be analysed here, if anonymization or
    # deanonymization is running
    return
    

if __name__ == '__main__':
    main()
