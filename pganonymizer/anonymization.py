"""Helper methods"""

from __future__ import absolute_import

import csv
import json
import logging
import re
import subprocess
import time

import psycopg2
import psycopg2.extras
from progress.bar import IncrementalBar
from psycopg2.errors import BadCopyFileFormat, InvalidTextRepresentation
from six import StringIO

from pganonymizer.constants import constants
from pganonymizer.exceptions import BadDataFormat
from pganonymizer.providers import get_provider
from pganonymizer.utils import _get_ids_sql_format, _, get_table_count, build_sql_select, update_fields_history


def anonymize_tables(connection, definitions, verbose=False):
    """
    Anonymize a list of tables according to the schema definition.

    :param connection: A database connection instance.
    :param list definitions: A list of table definitions from the YAML schema.
    :param bool verbose: Display logging information and a progress bar.
    """
    for definition in definitions:
        table_name = list(definition.keys())[0]
        table_definition = definition[table_name]
        columns = table_definition.get('fields', [])
        excludes = table_definition.get('excludes', [])
        search = table_definition.get('search')
        primary_key = table_definition.get('primary_key', constants.DEFAULT_PRIMARY_KEY)
        total_count = get_table_count(connection, table_name)
        res = build_data(connection, table_name, columns, excludes, total_count,search, primary_key, verbose)
        return res, table_name

def build_data(connection, table, columns, excludes, total_count, search,primary_key, verbose=False):
    """
    Select all data from a table and return it together with a list of table columns.

    :param connection: A database connection instance.
    :param str table: Name of the table to retrieve the data.
    :param list columns: A list of table fields
    :param list[dict] excludes: A list of exclude definitions.
    :param str search: A SQL WHERE (search_condition) to filter and keep only the searched rows.
    :param list history ids.
    :param int total_count: The amount of rows for the current table
    :param bool verbose: Display logging information and a progress bar.
    :return: A tuple containing the data list and a complete list of all table columns.
    :rtype: (list, list)
    """
    
    #todo 
    # umbauen der funktion
    # ausgangslage: bisher werden alle daten einer tabelle ermittelt, dann jeden record durchgegangen und alle anon felder
    # behandelt. Nachdem die komplette tabelle bearbeitet wurde, werden die history objekte angelegt. 
    # Anforderung: Alle Columns werden für die tabelle ermittelt und dann durchgegangen. Pro column werden 
    # dann die daten ermittelt. Nach bearbeitung eines records wird dann eine history angelegt. 
    if verbose:
        progress_bar = IncrementalBar('Anonymizing', max=total_count)

    cursor = build_sql_select(connection, 'ir_model', ["model = '{model_data}'".format(model_data=_(table))], select="id")
    table_id = cursor.fetchone()[0]
    cursor.close()
    cursor = build_sql_select(connection, table, search)
    number=0
    while True:
        rows = cursor.fetchmany(size=constants.ANON_FETCH_RECORDS)
        if not rows:
            print(str(constants.ANON_FETCH_RECORDS)+" more records anonymized!")
            break
        for row in rows:
            try:
                number=number+1
                row_column_dict = {}
                if not row_matches_excludes(row, excludes):
                    row_column_dict = get_column_values(row, columns, {'id':row.get('id'), 'table':table})
                    for key, value in row_column_dict.items():
                        original_data = {}
                        if row[key] == value:
                            continue
                        original_data[key] = {row.get('id'): row[key]}
                        import_data(connection, key, table, row.get('id'), primary_key, value)
                        if all(x1 in value for x1 in [table,key]):
                            run_anon('anon', connection, {table:original_data},  table_id)
            except Exception as ex:
                print(ex)
                
        if verbose:
            progress_bar.next()
    if verbose:
        progress_bar.finish()
    cursor.close()
    return number

def get_column_dict(columns):
    """
    Return a dictionary with all fields from the table definition and None as value.

    :param list columns: A list of field definitions from the YAML schema, e.g.:
        [
            {'first_name': {'provider': 'set', 'value': 'Foo'}},
            {'guest_email': {'append': '@localhost', 'provider': 'md5'}},
        ]
    :return: A dictionary containing all fields to be altered with a default value of None, e.g.::
        {'guest_email': None}
    :rtype: dict
    """
    column_dict = {}
    for definition in columns:
        column_name = list(definition.keys())[0]
        column_dict[column_name] = None
    return column_dict


def get_column_values(row, columns, row_info):
    """
    Return a dictionary for a single data row, with altered data.

    :param psycopg2.extras.DictRow row: A data row from the current table to be altered
    :param list columns: A list of table columns with their provider rules, e.g.:
        [
            {'guest_email': {'append': '@localhost', 'provider': 'md5'}}
        ]
    :return: A dictionary with all fields that have to be altered and their value for a single data row, e.g.:
        {'guest_email': '12faf5a9bb6f6f067608dca3027c8fcb@localhost'}
    :rtype: dict
    """
    column_dict = {}
    for definition in columns:
        column_name = list(definition.keys())[0]
        column_definition = definition[column_name]
        provider_config = column_definition.get('provider')
        orig_value = row.get(column_name)
        if not orig_value:
            # Skip the current column if there is no value to be altered
            continue
        provider = get_provider(provider_config)
        row_info.update({'field':column_name})
        value = provider.alter_value(orig_value, row_info)
        append = column_definition.get('append')
        if append:
            value = value + append
        column_dict[column_name] = value
    return column_dict

def truncate_tables(connection, tables):
    """
    Truncate a list of tables.

    :param connection: A database connection instance
    :param list[str] tables: A list of table names
    """
    if not tables:
        return
    cursor = connection.cursor()
    table_names = ', '.join(tables)
    logging.info('Truncating tables "%s"', table_names)
    cursor.execute(f'TRUNCATE TABLE {table_names} CASCADE;')
    # todo aufzeichungen
    #_run_query('truncate', connection, table_names)
    cursor.close()

def _get_anon_field_id(columns):
    dic = {}
    for column in columns:
        for key, value in column.items():
            dic[key] = value.get('provider').get('field_anon_id')
    return dic

def import_data(connection, field, source_table, row_id, primary_key, value):
    """
    Import the temporary and anonymized data to a temporary table and write the changes back.

    :param connection: A database connection instance.
    :param dict column_dict: A dictionary with all columns (specified by the schema definition) and a default value of
      None.
    :param str source_table: Name of the table to be anonymized.
    :param list table_columns: A list of all table columns.
    :param str primary_key: Name of the tables primary key.
    :param list data: The table data.
    """
    primary_key = primary_key if primary_key else constants.DEFAULT_PRIMARY_KEY
    cursor = connection.cursor()
    sql = f"UPDATE {source_table} SET {field} = '{value}' WHERE ID = {row_id}"
    cursor.execute(sql)
    cursor.close()
    
def row_matches_excludes(row, excludes=None):
    """
    Check whether a row matches a list of field exclusion patterns.

    :param list row: The data row
    :param list excludes: A list of field exclusion roles, e.g.:
        [
            {'email': ['\\S.*@example.com', '\\S.*@foobar.com', ]}
        ]
    :return: True or False
    :rtype: bool
    """
    excludes = excludes if excludes else []
    for definition in excludes:
        column = list(definition.keys())[0]
        for exclude in definition.get(column, []):
            result =  exclude_eval(exclude, column, row)
            if result:
                return result
    return False


def insert_migrated_fields_rec(cr, field, table):
    sql_insert = f"INSERT INTO {constants.TABLE_MIGRATED_FIELDS} (model_id, field_id) VALUES ('{table}', '{field}');"
    sql_select = f"SELECT id  from {constants.TABLE_MIGRATED_FIELDS} \
                            WHERE model_id = '{table}' \
                                   AND field_id = '{field}' \
                            LIMIT 1;"
    cr.execute(sql_select)
    record = cr.fetchone()
    if not record:
        cr.execute(sql_insert)

def exclude_eval(exclude, column, row):
    if column == "id":
        if row.get('id') == exclude:
            return True
    else:
        pattern = re.compile(exclude, re.IGNORECASE)
        if row[column] is not None and pattern.match(row[column]):
            return True

def createDataTable(table, con):
    cr = con.cursor()
    try:
        cr.execute(f'CREATE TABLE {constants.TABLE_MIGRATED_DATA}_{table} ( model_id CHAR(20),\
                                                                            field_id CHAR(20),\
                                                                            record_id INTEGER,\
                                                                            value CHAR(20),\
                                                                            state CHAR(20)\
                                                                            );')
    except:
        pass

def create_anon(con, data, table_id):
    cr = con.cursor()
    for table, field_data in data.items():
        createDataTable(table, con)
        # ids_sql_format = _get_ids_sql_format(ids)
        field = list(field_data.keys())[0]
        insert_migrated_fields_rec(cr, field, table)
        id = data.get(table).get(field)
        sql_migrated_data_insert = f"Insert into {constants.TABLE_MIGRATED_DATA}_{table} (model_id, field_id, record_id, value, state) VALUES (%s, %s, %s, %s, %s)"
        id = list(id.keys())[0]
        data = (table, field, id, data.get(table).get(field).get(id), 0)
        cr.execute(sql_migrated_data_insert, data)
        #update_fields_history(cr, table, id, "2", field)
    cr.close()

def run_anon(type, con, data, table_id):
    if type == 'anon':
        create_anon(con , data, table_id)
    elif type == 'truncate':
        create_truncate(con, data)
    
def create_truncate(con, data):
    cr = con.cursor()
    cr.close()