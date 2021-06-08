"""Helper methods"""

from __future__ import absolute_import

import csv
import json
import logging
import re
import subprocess

import psycopg2
import psycopg2.extras
from progress.bar import IncrementalBar
from psycopg2.errors import BadCopyFileFormat, InvalidTextRepresentation
from six import StringIO

from pganonymizer.constants import COPY_DB_DELIMITER, DEFAULT_PRIMARY_KEY
from pganonymizer.exceptions import BadDataFormat
from pganonymizer.providers import get_provider
from pganonymizer.revert import _run_query, _get_ids_sql_format, _


def anonymize_tables(connection, definitions, verbose=False):
    """
    Anonymize a list of tables according to the schema definition.

    :param connection: A database connection instance.
    :param list definitions: A list of table definitions from the YAML schema.
    :param bool verbose: Display logging information and a progress bar.
    """
    for definition in definitions:
        table_name = list(definition.keys())[0]
        #history_ids = get_history(connection, table_name)
        table_definition = definition[table_name]
        columns = table_definition.get('fields', [])
        excludes = table_definition.get('excludes', [])
        search = table_definition.get('search')
        primary_key = table_definition.get('primary_key', DEFAULT_PRIMARY_KEY)
        total_count = get_table_count(connection, table_name)
        build_data(connection, table_name, columns, excludes, total_count,search, primary_key, verbose)

def get_history(con, table):
    #todo checken ob es hier sinn macht über die history zu suchen
    cursor = con.cursor(cursor_factory=psycopg2.extras.DictCursor, name='fetch_large_result')
    sql_model_id = "SELECT id FROM ir_model where model ='{table}'".format(table=table.replace("_","."))
    sql = "select field_id, record_id from ir_model_fields_anonymization_history where state = '2' and model_id = ({sql_model_id}); ".format(sql_model_id = sql_model_id)
    cursor.execute(sql)
    history_data = []
    while True:
        records = cursor.fetchmany(size=2000)
        if not records:
            break
        for row in records:
            history_data.append((row.get('field_id'), row.get('record_id')))
    cursor.close()
    return history_data

def build_sql_select(connection, table, search, select="*"):
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    sql_select = "SELECT {select} FROM {table}".format(select=select,
                                                       table=table)
    if search:
        sql_statement = " AND ".join(search)
        sql = "{select} WHERE {search_condition};".format(select=sql_select, search_condition=sql_statement)
    else:
        sql = "{select};".format(select=sql_select)
    cursor.execute(sql)
    return cursor
     
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
    number=1
    while True:
        row = cursor.fetchone()
        if not row:
            break
            #print("record"+str(number)+" ("+str(number/total_number*100)+" %)")
        print(number)
        number=number+1
        original_data = {}
        row_column_dict = {}
        if not row_matches_excludes(row, excludes):
            row_column_dict = get_column_values(row, columns, {'id':row.get('id'), 'table':table})
            for key, value in row_column_dict.items():
                migrated_data = table.replace(".", "_")+"_"+key+"_"+str(row.get('id'))
                print(value)
                if value == migrated_data:
                    continue
                if not original_data.get(key):
                    original_data[key] = {}
                anon_field_id = columns[0].get(key).get('provider').get('field_anon_id')
                original_data[key].update({row.get('id'): row[key]})
                row[key] = value
                print("to be anonymized")
                import_data(connection, key, table, row.get('id'), primary_key, value)
                _run_query('anon', connection, {table:original_data}, [anon_field_id], table_id)
        if verbose:
            progress_bar.next()
        
        #data.append(row.values())
        # todo update stuff
        
    if verbose:
        progress_bar.finish()
    cursor.close()
    
# def row_check_history(columns, search, history=False):
#     field = list(columns[0].keys())[0]
#     anon_field_id = columns[0].get(field).get('provider').get('field_anon_id')
#     if history and len(history)>0:
#         history = [ x for x in history if x[0]==anon_field_id]
#         not_id = [id[1] for id in history]
#         not_id = _get_ids_sql_format(not_id)
#         if not_id:
#             if search:
#                 search = search.append("id not in {id_list}".format(id_list=not_id))
#             else:
#                 search = ["id not in {id_list}".format(id_list=not_id)]
#     return search, anon_field_id, field


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

def exclude_eval(exclude, column, row):
    if column == "id":
        if row.get('id') == exclude:
            return True
    else:
        pattern = re.compile(exclude, re.IGNORECASE)
        if row[column] is not None and pattern.match(row[column]):
            return True

def copy_from(connection, data, table, columns):
    """
    Copy the data from a table to a temporary table.

    :param connection: A database connection instance.
    :param list data: The data of a table.
    :param str table: Name of the temporary table used for copying the data.
    :param list columns: All columns of the current table.
    :raises BadDataFormat: If the data cannot be imported due to a invalid format.
    """
    new_data = data2csv(data)
    cursor = connection.cursor()
    try:
        cursor.copy_from(new_data, table, sep=COPY_DB_DELIMITER, null='\\N', columns=columns)
    except (BadCopyFileFormat, InvalidTextRepresentation) as exc:
        raise BadDataFormat(exc)
    cursor.close()


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
    primary_key = primary_key if primary_key else DEFAULT_PRIMARY_KEY
    #temp_table = '"tmp_{table}"'.format(table=source_table)
    cursor = connection.cursor()
    sql = "UPDATE {table} SET {field} = '{value}' WHERE ID = {id}".format(table=source_table,
                                                                          field=field,
                                                                          value=value,
                                                                          id=row_id)
    #cursor.execute('CREATE TEMP TABLE %s (LIKE %s INCLUDING ALL) ON COMMIT DROP;' % (temp_table, source_table))
    #cursor.execute('COMMIT;')
    #copy_from(connection, data, temp_table, table_columns)
    #set_columns = ', '.join(['{column} = s.{column}'.format(column='"{}"'.format(key)) for key in column_dict.keys()])
    #sql = (
    #    'UPDATE {table} t '
    #    'SET {columns} '
    #    'FROM {source} s '
    #    'WHERE t.{primary_key} = s.{primary_key};'
    #).format(table=source_table, columns=set_columns, source=temp_table, primary_key=primary_key)
    cursor.execute(sql)
    #cursor.execute('DROP TABLE %s;' % temp_table)
    cursor.close()


def get_connection(pg_args):
    """
    Return a connection to the database.

    :param pg_args:
    :return: A psycopg connection instance
    :rtype: psycopg2.connection
    """
    return psycopg2.connect(**pg_args)


def get_table_count(connection, table):
    """
    Return the number of table entries.

    :param connection: A database connection instance
    :param str table: Name of the database table
    :return: The number of table entries
    :rtype: int
    """
    sql = 'SELECT COUNT(*) FROM {table};'.format(table=table)
    cursor = connection.cursor()
    cursor.execute(sql)
    total_count = cursor.fetchone()[0]
    cursor.close()
    return total_count


def data2csv(data):
    """
    Return a string buffer, that contains delimited data.

    :param list data: A list of values
    :return: A stream that contains tab delimited csv data
    :rtype: StringIO
    """
    buf = StringIO()
    writer = csv.writer(buf, delimiter=COPY_DB_DELIMITER, lineterminator='\n', quotechar='~')
    for row in data:
        row_data = []
        for x in row:
            if x is None:
                val = '\\N'
            elif type(x) == str:
                val = x.strip()
            elif type(x) == dict:
                val = json.dumps(x)
            else:
                val = x
            row_data.append(val)
        writer.writerow(row_data)
    buf.seek(0)
    return buf


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
    cursor.execute('TRUNCATE TABLE {tables} CASCADE;'.format(tables=table_names))
    # todo aufzeichungen
    #_run_query('truncate', connection, table_names)
    cursor.close()


def create_database_dump(filename, db_args):
    """
    Create a dump file from the current database.

    :param str filename: Path to the dumpfile that should be created
    :param dict db_args: A dictionary with database related information
    """
    arguments = '-d {dbname} -U {user} -h {host} -p {port}'.format(**db_args)
    cmd = 'pg_dump -p -Fc -Z 9 {args} -f {filename}'.format(
        args=arguments,
        filename=filename
    )
    logging.info('Creating database dump file "%s"', filename)
    subprocess.run(cmd, shell=True)
