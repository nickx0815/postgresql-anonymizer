"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import logging
import sys
import time
from time import sleep
import threading, time
from queue import Queue

import yaml

from pganonymizer.constants import DATABASE_ARGS, DEFAULT_SCHEMA_FILE, NUMBER_MAX_THREADS
from pganonymizer.providers import PROVIDERS
from pganonymizer.utils import anonymize_tables, create_database_dump, get_connection, truncate_tables
from pganonymizer.revert import run_revert

jobs = Queue()

class BaseMain():
    pass
    
class AnonymizationMain(BaseMain):
    pass

class DeAnonymizationMain(BaseMain):
    pass

def main():
    #todo needs to be implemented, run the script via command line. 
    # the args need to be analysed here, if anonymization or
    # deanonymization is running
    return

def get_pg_args(args):
    """
    Map all commandline arguments with database keys.

    :param argparse.Namespace args: The commandline arguments
    :return: A dictionary with database arguments
    :rtype: dict
    """
    return ({name: value for name, value in
             zip(DATABASE_ARGS, (args.dbname, args.user, args.password, args.host, args.port))})


def list_provider_classes():
    """List all available provider classes."""
    print('Available provider classes:\n')
    for provider_cls in PROVIDERS:
        print('{:<10} {}'.format(provider_cls.id, provider_cls.__doc__))

def get_args():
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

def _get_run_data(args):
    if not args:
        args = get_args()
    pg_args = get_pg_args(args)
    return pg_args, args

def main_deanonymize(args=None):
    pg_args, args = _get_run_data(args)
    connection = get_connection(pg_args)
    run_revert(connection, args)
    return False

def get_schema_batches(schema):
    for type_, type_attributes in schema.items():
        for table in type_attributes:
            if type(table) == str:
                jobs.put({type_: [table]})
            else:
                for table_key, table_attributes in table.items():
                    fields = table_attributes['fields']
                    for field in fields:
                        for field_key, field_attributes in field.items():
                            jobs.put({type_: [{table_key:{'fields':[{field_key:field_attributes}]}}]})

def main_anonymize(args=None):
    """Main method"""
    # own connection per schema batch...
    pg_args, args = _get_run_data(args)

    loglevel = logging.WARNING
    if args.verbose:
        loglevel = logging.DEBUG
    logging.basicConfig(format='%(levelname)s: %(message)s', level=loglevel)

    if args.list_providers:
        list_provider_classes()
        sys.exit(0)

    schema = yaml.load(open(args.schema), Loader=yaml.FullLoader)
    get_schema_batches(schema)
    queue_size = jobs.qsize()
    number_threads = queue_size if queue_size < NUMBER_MAX_THREADS else NUMBER_MAX_THREADS
    print("Number of threads started: {number}".format(number=number_threads))
    for schema in range(number_threads):
        worker = threading.Thread(target=start_thread, args=(jobs,args, pg_args))
        worker.start()
    
    print("waiting for queue to complete tasks")
    jobs.join()
    print("all done")

def start_thread(q, args, pg_args):
    while not q.empty():
        start_time = time.time()
        schema_batch = q.get()
        connection = get_connection(pg_args)
        try:
            #todo implement truncate functionality, not working right now
            #truncate_tables(connection, schema_batch.get('truncate', []))
            anonymize_tables(connection, schema_batch.get('tables', []), verbose=args.verbose)
            if not args.dry_run:
                connection.commit()
            end_time = time.time()
            logging.info('Anonymization took {:.2f}s'.format(end_time - start_time))
        except Exception as ex:
            logging.info(ex)
        connection.close()
        q.task_done()
#     if args.dump_file:
#         create_database_dump(args.dump_file, pg_args)
    



if __name__ == '__main__':
    main()
