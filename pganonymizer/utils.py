"""Helper methods"""


import csv
import json

import psycopg2
import psycopg2.extras
from psycopg2.errors import BadCopyFileFormat, InvalidTextRepresentation
from six import StringIO

from pganonymizer.constants import constants
from pganonymizer.exceptions import BadDataFormat
no_pretty_table = False
try:
    from prettytable import PrettyTable
except:
    no_pretty_table = True


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

def build_sql_select(connection, table, search, select="*", operator="AND"):
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    sql_select = "SELECT {select} FROM {table}".format(select=select,
                                                       table=table)
    if search:
        sql_statement = f" {operator} ".join(search)
        sql = "{select} WHERE {search_condition};".format(select=sql_select, search_condition=sql_statement)
    else:
        sql = "{select};".format(select=sql_select)
    cursor.execute(sql)
    return cursor

def convert_list_to_sql(ids, char=False):
    if ids:
        parsed =  str(set([x for x in ids])).replace("{", "(").replace("}", ")")
        if not char:
            return parsed.replace("'","")
        return parsed
    return False

def get_distinct_from_tuple(iterable, index, add_index=False):
    distinct_table_dic = {}
    for object in iterable:
        current_object = object[index]
        if current_object not in distinct_table_dic.keys():
            distinct_table_dic[current_object] = []
        if add_index:
            add_object = object[add_index]
        else:
            add_object = object
        distinct_table_dic[current_object].append(add_object)
    return distinct_table_dic

def create_basic_table(con, tables=constants.BASIC_TABLES, suffix=""):
    cr = con.cursor()
    for basic_table in tables:
        basic_table_with_suffix = f'{basic_table}{suffix}'
        cr.execute(f"select exists ( select from information_schema.tables where table_name = '{basic_table_with_suffix}');")
        table = cr.fetchone()
        if not table[0]:
            fields = constants.TABLE_MIGRATED_DEFINITON.get(basic_table)
            if fields:
                cr.execute(f'CREATE TABLE {basic_table_with_suffix} {convert_list_to_sql(fields)};')
    cr.execute('COMMIT;')
    cr.close()
    con.close()

def get_migration_mapping(con, table, fields):
    # todo function to determine which mapping (10,11,12...)
    cr = con.cursor()
    list = []
        #field_parsed = convert_list_to_sql(fields, char=True)
    for field in fields:
        select_model_id_sql = f"SELECT old_model_name, new_model_name, old_field_name, new_field_name FROM {constants.TABLE_MIGRATED_DATA_MAPPING} where old_model_name = '{table}' and old_field_name = '{field}';"
        cr.execute(select_model_id_sql)
        record = cr.fetchone()
        if record:
            list.append(record)
    if not list:
        return False
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

def run_analysis(con, db, tables=False):
    cursor = con.cursor()
    cursor2 = con.cursor()
    if not tables:
        orig_tables = "like '%'"
    else:
        orig_tables = f"in {convert_list_to_sql(tables, char=True)}"
    cursor.execute(f"select table_name from information_schema.tables where table_name {orig_tables} and table_type = 'BASE TABLE' and table_schema = 'public' and table_catalog = '{db}';")
    orig_tables = cursor.fetchall()
    for table in orig_tables:
        anonymizable_fields = []
        anonymized_fields = []
        non_anonymized_fields = []
        info_table = table[0]
        #TODO suche muss angepasst werden, es werden felder gefunden welche bei der suche auf der tabelle dann nicht exisiieren
        # muss schauen wie ich die where clause anpassen muss
        cursor.execute(f" SELECT column_name FROM information_schema.columns WHERE data_type in ('text', 'character varying') and table_name = '{info_table}';")
        while True:
            field_name = cursor.fetchall()
            if not field_name:
                break
            if not no_pretty_table:
                x = PrettyTable()
                x.field_names = ["Fieldname", "Percent Anonymized", "Total Records", "Total Anonymized Records"]
            else:
                x = "Fieldname    Percent Anonymized    Total Records    Total Anonymized Records\n"
            for field in field_name:
                anonymizable_fields.append(field)
                field = field[0]
                cursor2.execute(f"select exists (select * from {info_table} where \"{field}\" like '{info_table}_{field}%');")
                result = cursor2.fetchone()
                if not result[0]:
                    non_anonymized_fields.append(field)
                    continue
                if result[0]:
                    anonymized_fields.append(field)
                    cursor.execute(f"select count(*) from {info_table} where {field} is not NULL and {field} <> '';")
                    total_number = cursor.fetchone()[0]
                    cursor.execute(f"select count(*) from {info_table} where {field} like '{info_table}_{field}%' \
                        and {field} is not NULL and {field} <> '';")
                    anonymized_number = cursor.fetchone()[0]
                    percent = round(anonymized_number/total_number*100,2) if total_number != 0 else 0.0
                    if not no_pretty_table:
                        x.add_row([field, percent, total_number, anonymized_number])
                    else:
                        x = x + f"{field}, {percent} %, {total_number}, {anonymized_number}\n"
            if len(anonymized_fields) != 0:
                print(f"{info_table}")
                print(x)
                print(f"number fields {len(anonymized_fields)}")
                print("")
    print("finished")
