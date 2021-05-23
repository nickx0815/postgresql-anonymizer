"""Commandline implementation"""

from __future__ import absolute_import, print_function

import argparse
import logging
import sys
import time

import yaml

from pganonymizer.constants import DATABASE_ARGS, DEFAULT_SCHEMA_FILE
from pganonymizer.providers import PROVIDERS
from pganonymizer.utils import anonymize_tables, create_database_dump, get_connection, truncate_tables
from pganonymizer.revert import run_revert


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
    connection, args = _get_run_data(args)
    run_revert(connection, args)
    return False

def get_schema_batches(schema):
    #todo batches basteln
    # idee: list von einzelnen schema, welche immer nur ein table und in feld beinhalten
    # so müssen die nachfolgenden funktionen nicht angepasst werden
    schema_batches = []
    for type, type_attributes in schema:
        for table in type_attributes:
            for table_key, table_attributes in table.items():
                fields = table_attributes['fields']
                for field in fields:
                    for field_key, field_attributes in field.items():
                        schema_batches.append({type:{table_key:{'fields':[{field_key:field_attributes}]}}})
    return schema_batches

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
    schema_batches = get_schema_batches(schema)
    for schema_batch in schema_batches:
        connection = get_connection(pg_args)
        start_time = time.time()
        truncate_tables(connection, schema_batch.get('truncate', []))
        anonymize_tables(connection, schema_batch.get('tables', []), verbose=args.verbose)
        if not args.dry_run:
            connection.commit()
        end_time = time.time()
        logging.info('Anonymization took {:.2f}s'.format(end_time - start_time))
        connection.close()
    if args.dump_file:
        create_database_dump(args.dump_file, pg_args)
    



# if __name__ == '__main__':
#     main()
