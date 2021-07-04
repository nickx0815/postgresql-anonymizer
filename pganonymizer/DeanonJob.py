"""Commandline implementation"""

from __future__ import absolute_import, print_function

import time


from pganonymizer.constants import constants 
from pganonymizer.utils import build_sql_select, _get_mapped_data
from pganonymizer.DeanonProcessing import DeanonProcessing
from pganonymizer.MainJob import BaseJobClass

class DeanonJobClass(BaseJobClass):
    THREAD = "NUMBER_MAX_THREADS_DEANON"
    tables = []
    TMPconnection = {}
    
    def set_tables(self, table):
        self.tables = table
    
    def get_tables(self):
        return self.tables
    
    def set_tmp_connection(self, con):
        self.TMPconnection = con
    
    def get_tmp_connection(self):
        return self.TMPconnection
    
    def create_tmp_tables(self):
        schema = self.get_schema()
        connection = self.get_connection()
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
        self.set_tables(list_table)
        crtest.close()
        self.set_tmp_connection(connection)
    
    def update_queue(self):
        self.create_tmp_tables()
        self.__update_queue()
    
    def __update_queue(self):
        connection = self.get_connection(autocommit=True)
        #todo umbauen, dass ein job jeweils alle migrated_fields eines records beinhaltet. 
        #todo weitere deanon methoden umbaunen, sodass alle felder mit einem update deanonymsiert werden
        crtest = connection.cursor()
        for table, fields in self.schema.items():
            for field in fields:
                cursor = build_sql_select(connection, constants.TABLE_MIGRATED_DATA+"_"+table, 
                                                                    ["field_id = '{field_id}'".format(field_id=field),
                                                                    "state = 0"],
                                                                    select="id, record_id, value")
                while True:
                    list = []
                    records = cursor.fetchmany(size=constants.DEANON_NUMBER_FIELD_PER_THREAD)
                    totalrecords = len(records)
                    if not records:
                        break
                    for rec in records:
                        list.append((rec.get('record_id'), rec.get('value'), rec.get('id')))
                    self.jobs.put(DeanonProcessing(self, self.TMPconnection, totalrecords, (field, list), table, self.pg_args,'deanon'))
                crtest.close()
        connection.close()
        
    def start_processing(self):
        super(BaseJobClass, self).start_processing()
        self.TMPconnection.close()
        
        