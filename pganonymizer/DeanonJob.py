"""Commandline implementation"""

from __future__ import absolute_import, print_function

import time


from pganonymizer.constants import constants 
from pganonymizer.utils import  get_connection, build_sql_select
from pganonymizer.DeanonProcessing import run_revert, _get_mapped_data
from pganonymizer.MainJob import BaseMain

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
                cursor = build_sql_select(connection, constants.TABLE_MIGRATED_DATA+"_"+table, 
                                                                    ["field_id = '{field_id}'".format(field_id=field),
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
        
        