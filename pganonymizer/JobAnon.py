"""Helper methods"""


import re

import psycopg2
import psycopg2.extras
from progress.bar import IncrementalBar
from psycopg2.errors import BadCopyFileFormat, InvalidTextRepresentation
from six import StringIO

from pganonymizer.constants import constants
from pganonymizer.providers import get_provider
from pganonymizer.utils import get_table_count, build_sql_select
from pganonymizer.Job import Job
from pganonymizer.logging import logger
logging_ = logger()



class JobAnon(Job):
    #todo verschlüsselung einbauen
    
    _autocommit = False
    
    def __init__(self, main_job, type, totalrecords, data, table):
        super(JobAnon, self).__init__(main_job, totalrecords, data, table, type)
        self.migration = self.main_job.args.migration
        
    def _get_run_method(self):
        return constants.PROCESS_METHOD_MAPPING[self.type]
    
    def anonymize_tables(self, connection):
        """
        Anonymize a list of tables according to the schema definition.
    
        :param connection: A database connection instance.
        :param list definitions: A list of table definitions from the YAML schema.
        :param bool verbose: Display logging information and a progress bar.
        """
        definition = self.schema
        table_name = self.table
        columns = definition.get('fields', [])
        excludes = definition.get('excludes', [])
        search = definition.get('search')
        primary_key = definition.get('primary_key', constants.DEFAULT_PRIMARY_KEY)
        self.__process(connection, table_name, columns, excludes,search, primary_key)
    
    def __process(self, connection, table, columns, excludes, search, primary_key):
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
        cursor = build_sql_select(connection, table, search)
        rows = cursor.fetchall(back_as=[])
        for row in rows:
            #todo check if try can be removed
            try:
                row_column_dict = {}
                if not self.row_matches_excludes(row, excludes):
                    row_column_dict = self.get_column_values(row, columns, {'id':row.get('id'), 'table':table})
                    for key, value in row_column_dict.items():
                        original_data = {}
                        if row[key] == value:
                            #the case for already anonymized (migration) records
                            continue
                        original_data[key] = {row.get('id'): row[key]}
                        try:
                            if self.migration == 'True':
                                self.save_original_data(connection, table, original_data)
                            self.migrate_field(connection, key, table, row.get('id'), primary_key, value)
                            self.updatesuccessfullfields()
                        except:
                            connection.rollback()
                        finally:
                            connection.commit()
                    #todo nur updaten wenn auhc irgendein feld bearbeitet wurde
                    self.updatesuccessfullrecords()
            except Exception as ex:
                #todo use logger
                print(ex)
        cursor.close()
    
    def get_column_dict(self, columns):
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
    
    def get_column_values(self, row, columns, row_info):
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
            values = {'value':orig_value, 'table': row_info.get('table'), 
                                            'field': column_name, 'id':row_info.get('id')}
            values.update(provider_config)
            value = provider.alter_value(**values)
            append = column_definition.get('append')
            if append:
                value = value + append
            column_dict[column_name] = value
        return column_dict
    
    @logging_.TRUNCATE_TABLES
    def truncate_tables(self, connection):
        """
        Truncate a list of tables.
    
        :param connection: A database connection instance
        :param list[str] tables: A list of table names
        """
        tables = self.schema
        if not tables:
            return
        cursor = connection.cursor()
        for table in tables:
            cursor.execute(f'DELETE FROM {table};')
            self.totalrecords = self.totalrecords+1
        cursor.close()
    
    @logging_.ANONYMIZATION_RECORD
    def migrate_field(self, connection, field, source_table, row_id, primary_key, value):
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
    
    @logging_.EXCLUDE_RECORD 
    def row_matches_excludes(self, row, excludes=None):
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
                result =  self.exclude_eval(exclude, column, row)
                if result:
                    return result
        return False
    
    @logging_.CHECK_MIGRATED_FIELD
    def save_migrated_field(self, cr, field, table):
        sql_insert = f"INSERT INTO {constants.TABLE_MIGRATED_FIELDS} (model_id, field_id) VALUES ('{table}', '{field}');"
        sql_select = f"SELECT id  from {constants.TABLE_MIGRATED_FIELDS} \
                                WHERE model_id = '{table}' \
                                       AND field_id = '{field}' \
                                LIMIT 1;"
        cr.execute(sql_select)
        record = cr.fetchone()
        if not record:
            self.insert_migrated_fields_rec(cr, sql_insert)
    
    @logging_.INSERT_MIGRATED_FIELD
    def insert_migrated_fields_rec(self, cr, insert_sql):
        try:
            cr.execute(insert_sql)
            return True, False
        except Exception:
            return False, Exception
    
    def exclude_eval(self, exclude, column, row):
        if column == "id":
            if row.get('id') == exclude:
                return True
            return False
        else:
            pattern = re.compile(exclude, re.IGNORECASE)
            if row[column] is not None and pattern.match(row[column]):
                return True
            return False
    
    @logging_.INSERT_MIGRATED_DATA
    def save_original_data(self, con, table, data):
        cr = con.cursor()
        field = list(data.keys())[0]
        self.save_migrated_field(cr, field, table)
        id = data.get(field)
        sql_migrated_data_insert = f"Insert into {constants.TABLE_MIGRATED_DATA}{table} \
                                        (field_id, record_id, value, state)\
                                         VALUES (%s, %s, %s, %s)"
        id = list(id.keys())[0]
        data = (field, id, data.get(field).get(id), 0)
        cr.execute(sql_migrated_data_insert, data)
        cr.close()
        
        