"""Helper methods"""

from __future__ import absolute_import

import csv
import json
import logging
import subprocess
import time

import psycopg2, datetime
import psycopg2.extras
from progress.bar import IncrementalBar
from psycopg2.errors import BadCopyFileFormat, InvalidTextRepresentation
from six import StringIO

from pganonymizer.constants import constants
from pganonymizer.exceptions import BadDataFormat
from pganonymizer.providers import get_provider


def _(t):
    return t.replace("_", ".")



def get_pg_args(args):
        """
        Map all commandline arguments with database keys.
    
        :param argparse.Namespace args: The commandline arguments
        :return: A dictionary with database arguments
        :rtype: dict
        """
        return ({name: value for name, value in
                 zip(constants.DATABASE_ARGS, (args.dbname, args.user, args.password, args.host, args.port))})

def update_fields_history(cr, model_id, record, state, field_id):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr.execute(f"Insert into ir_model_fields_anonymization_history ( \
                state, model_id, field_to_group,create_date, write_date, create_uid, record_id \
            ) values ( \
                {state}, '{model_id}','{field_id}', '{str(now)}', '{str(now)}', {1}, {record});")

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

def _get_ids_sql_format(ids, char=False):
    if ids:
        parsed =  str(set([x for x in ids])).replace("{", "(").replace("}", ")")
        if not char:
            parsed.replace("'", "")
        return parsed
    return False

def get_distinct_from_tuple(iterable, index):
    distinct_table_dic = {}
    for object in iterable:
        current_object = object[index]
        if current_object not in distinct_table_dic.keys():
            distinct_table_dic[current_object] = []
        distinct_table_dic[current_object].append(current_object[3])
    return distinct_table_dic

def create_basic_tables(con, tables=constants.BASIC_TABLES, suffix=""):
    cr = con.cursor()
    for basic_table in tables:
        basic_table_with_suffix = f'{basic_table}{suffix}'
        cr.execute(f"select exists ( select from information_schema.tables where table_name = '{basic_table_with_suffix}');")
        table = cr.fetchone()
        if not table[0]:
            fields = constants.TABLE_MIGRATED_DEFINITON.get(basic_table)
            if fields:
                cr.execute(f'CREATE TABLE {basic_table_with_suffix} {_get_ids_sql_format(fields)};')
    cr.execute('COMMIT;')
    cr.close()
    con.close()

def _get_mapped_data(con, table, fields):
    # todo function to determine which mapping (10,11,12...)
    cr = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    list = []
    for field in fields:
        #field_parsed = _get_ids_sql_format(fields, char=True)
        select_model_id_sql = f"SELECT old_model_name, new_model_name, old_field_name, new_field_name FROM {constants.TABLE_MIGRATED_DATA_MAPPING} where old_model_name = '{table}' and old_field_name in {field}"
        cr.execute(select_model_id_sql)
        while True:
            records = cr.fetchone()
            if not records:
                list.append(table,table,field,field)
            for record in records:
                list.append(record)
    return list
     
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
        cursor.copy_from(new_data, table, sep=constants.COPY_DB_DELIMITER, null='\\N', columns=columns)
    except (BadCopyFileFormat, InvalidTextRepresentation) as exc:
        raise BadDataFormat(exc)
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
    writer = csv.writer(buf, delimiter=constants.COPY_DB_DELIMITER, lineterminator='\n', quotechar='~')
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
# 
# def create_database_dump(db_args):
#     """
#     Create a dump file from the current database.
# 
#     :param str filename: Path to the dumpfile that should be created
#     :param dict db_args: A dictionary with database related information
#     """
#     cur_time = time.time()
#     dbname = db_args.get('dbname')
#     file = f"{constants.PATH_DUMP}{cur_time}_{dbname}"
#     args = '{dbname}'.format(**db_args)
#     cmd = f'docker exec -i migration_postgres_1 psql -U odoo {args} > {file}'
#     logging.info('Creating database dump file "%s"', file)
#     subprocess.run(cmd, shell=True)
