"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import logging
import sys
import copy
import time
import datetime
import psycopg2
from psycopg2 import OperationalError
from time import sleep
import threading, time
from queue import Queue

import yaml

from pganonymizer.constants import constants 
from pganonymizer.providers import PROVIDERS
from pganonymizer.anonymization import anonymize_tables
from pganonymizer.utils import create_database_dump, get_connection, build_sql_select, _get_ids_sql_format,get_pg_args
from pganonymizer.deanonymization import run_revert, _get_mapped_data
from symbol import except_clause


class BaseMain():
    jobs = Queue()
    number_rec = {}
    schema = False
    pg_args = False
    
    def isStartingUpError(self, oe):
        if constants.STARTINGUPERROR in oe.args[0]:
            return True
        return False
    
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
    
class AnonymizationMain(BaseMain):
    
    THREAD = "NUMBER_MAX_THREADS_ANON"
    
    def get_args(self):
        parser =  BaseMain.get_args(self, parseArgs=False)
        parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity')
        parser.add_argument('-l', '--list-providers', action='store_true', help='Show a list of all available providers',
                            default=False)
        parser.add_argument('--dump-file', help='Create a database dump file with the given name')
        args = parser.parse_args()
        return args
    
    
    def startprocessing(self, args_):
        loglevel = logging.WARNING
        if args_.verbose:
            loglevel = logging.DEBUG
        logging.basicConfig(format='%(levelname)s: %(message)s', level=loglevel)
        if args_.list_providers:
            self.list_provider_classes()
            sys.exit(0)
        BaseMain.startprocessing(self, args_)
    
    def update_queue(self):
        #todo konfigurierbar
        #search wird nicht übernommen
        pg_args = self.pg_args
        connection = get_connection(pg_args)
        schema = self.schema
        for type_, type_attributes in schema.items():
            for table in type_attributes:
                if type(table) == str:
                    self.jobs.put({type_: [table]})
                else:
                    for table_key, table_attributes in table.items():
                        number = 0
                        table_attributes = self.update_anon_search(table_key, table_attributes)
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
        connection.close()
    
    def update_anon_search(self, table, table_attributes):
        searchList = table_attributes.get('search')
        if searchList:
            searchList.append('')
        return table_attributes
        
                        
    def print_info(self, table, total, anonymized, percent_anonymized):
        percent="{:.2f}".format(percent_anonymized*100)
        print(f"Table {table} is {percent} % anonymized")
        total_anonymized = percent_anonymized*total
        total=total_anonymized-(total_anonymized%1)
        print(f"Total Records anonymized {total}")
        if percent_anonymized == 1:
            runtime = time.time()-self.number_rec[table][2]
            time_ = str(datetime.timedelta(seconds=runtime))
            print(f"Table {table} is anonymized")
            print(f"Anonymization of {table} took {time_}")
        self.number_rec[table] = (total, anonymized, self.number_rec[table][2])
    
    def _runSpecificTask(self, args, schema):
        pg_args = self.pg_args
        connection = get_connection(pg_args)
        connection.autocommit = True
        try:
            res, table = anonymize_tables(connection, schema.get('tables', []), verbose=args.verbose)
            total_size = self.number_rec[table][0]
            number_anonymized = self.number_rec[table][1]+res
            percent_anonymized = number_anonymized/total_size
            self.print_info(table, total_size, number_anonymized, percent_anonymized)
        except Exception as ex:
            print(ex)
        finally:
            connection.close()

class DeAnonymizationMain(BaseMain):
    THREAD = "NUMBER_MAX_THREADS_DEANON"
    
    tables = []
    TMPconnection = {}
    
    def createTmpTables(self):
        pg_args = self.pg_args
        connection = get_connection(pg_args)
        schema = self.schema
        connection.autocommit = True
        #todo umbauen, dass ein job jeweils alle migrated_fields eines records beinhaltet. 
        #todo weitere deanon methoden umbaunen, sodass alle felder mit einem update deanonymsiert werden
        crtest = connection.cursor()
        list_table = []
        for table, fields in schema.items():
            mapped_field_data = _get_mapped_data(connection, table)
            migrated_table = mapped_field_data[1]
            temp_table = "tmp_"+migrated_table
            list_table.append(temp_table)
            fields_string = ",".join(fields+['id'])
            crtest.execute(f'CREATE TEMPORARY TABLE {temp_table} AS SELECT {fields_string} FROM {migrated_table};' )
            crtest.execute(f"CREATE INDEX index_id ON {temp_table} (id);")
            for field in fields:
                mapped_field_data = _get_mapped_data(connection, table, field=field)
                migrated_field = mapped_field_data[3]
                crtest.execute(f"CREATE INDEX index_{migrated_field} ON {temp_table} ({field});")
        self.tables = list_table
        crtest.close()
        self.TMPconnection = connection
    
    def update_queue(self):
        self.createTmpTables()
        self.__update_queue()
    
    def __update_queue(self):
        pg_args = self.pg_args
        connection = get_connection(pg_args)
        schema = self.schema
        connection.autocommit = True
        #todo umbauen, dass ein job jeweils alle migrated_fields eines records beinhaltet. 
        #todo weitere deanon methoden umbaunen, sodass alle felder mit einem update deanonymsiert werden
        crtest = connection.cursor()
        for table, fields in schema.items():
            for field in fields:
                cursor = build_sql_select(connection, constants.TABLE_MIGRATED_DATA, 
                                                                    ["model_id = '{model_id}'".format(model_id=table),
                                                                    "field_id = '{field_id}'".format(field_id=field),
                                                                    "state = 0"],
                                                                    select="id, record_id, value")
                while True:
                    list = []
                    records = cursor.fetchmany(size=constants.DEANON_NUMBER_FIELD_PER_THREAD)
                    if not records:
                        break
                    for rec in records:
                        list.append((rec.get('record_id'), rec.get('value'), rec.get('id')))
                    self.jobs.put({table: (field, list)})
                crtest.close()
        connection.close()
        

    def _runSpecificTask(self, args, data):
        pg_args = self.pg_args
        connection = get_connection(pg_args)
        connection.autocommit = True
        try:
            start_time = time.time()
            run_revert(connection, args, data, self.TMPconnection)
            end_time = time.time()
            print('Deanonymization took {:.2f}s'.format(end_time - start_time))
        except Exception as ex:
            print(ex)
        finally:
            connection.close()
    
    def startprocessing(self, args_):
        BaseMain.startprocessing(self, args_)
        self.TMPconnection.close()
            
def main():
    #todo needs to be implemented, run the script via command line. 
    # the args need to be analysed here, if anonymization or
    # deanonymization is running
    return
    

if __name__ == '__main__':
    main()
