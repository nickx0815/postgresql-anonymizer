"""Commandline implementation"""


from pganonymizer.constants import constants 
from pganonymizer.utils import build_sql_select, get_migration_mapping, get_distinct_from_tuple
from pganonymizer.DeanonProcessing import DeanonProcessing
from pganonymizer.MainJob import BaseJobClass

class DeanonJobClass(BaseJobClass):
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
        crtest = connection.cursor()
        list_table = []
        for table, fields in schema.items():
            mapped_field_data = get_migration_mapping(connection, table, fields=fields)
            distinct_tables = get_distinct_from_tuple(mapped_field_data, 1)
            for migrated_table, mapped_fields in distinct_tables.items():
                temp_table = "tmp_"+migrated_table
                fields_string = ",".join(mapped_fields+['id'])
                try:
                    crtest.execute(f'CREATE TEMPORARY TABLE {temp_table} AS SELECT {fields_string} FROM {migrated_table};' )
                    crtest.execute(f"CREATE INDEX index_id ON {temp_table} (id);")
                except:
                    #for the case that 2 table in schema are refering to one table in the migrated db. So the tmp table is already existing
                    pass
                list_table.append(temp_table)
                for field in mapped_fields:
                    crtest.execute(f"CREATE INDEX index_{field} ON {temp_table} ({field});")
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
                cursor = build_sql_select(connection, f"{constants.TABLE_MIGRATED_DATA}{table}", 
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
                    self.jobs.put(DeanonProcessing(self, self.TMPconnection, totalrecords, (field, list), table, 'deanon'))
                crtest.close()
        connection.close()
        
    def start_processing(self):
        super(DeanonJobClass, self).start_processing()
        self.TMPconnection.close()
        
        