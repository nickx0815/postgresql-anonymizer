import psycopg2
from pganonymizer.utils import update_fields_history, _get_mapped_data
from pganonymizer.constants import constants

from pganonymizer.MainProcessing import MainProcessing

class DeanonProcessing(MainProcessing):
    type = "deanonymization"
    
    def __init__(self, tmpconnection, totalrecords, schema, table, pg_args, logger):
        super(DeanonProcessing, self).__init__(totalrecords, schema, table, pg_args, logger)
        self.tmpcon = tmpconnection
        
    def _get_rel_method(self):
        return "run_revert"

    def run_revert(self, connection):
        data = self.schema
        table = self.table
        number = 0
        mapped_field_data = _get_mapped_data(connection, table, field=data[0])
        original_table = mapped_field_data[0]
        migrated_table = mapped_field_data[1]
        original_field = mapped_field_data[2]
        migrated_field = mapped_field_data[3]
        for id, value, record_id in data[1]:
            number = number + 1
            cr3 = self.tmpcon.cursor(cursor_factory=psycopg2.extras.DictCursor)
            orig_value = original_table + "_" + original_field + "_" + str(id)
            record_db_id_sql = f"SELECT ID FROM {'tmp_'+migrated_table} where {migrated_field} = '{orig_value}';"
            cr3.execute(record_db_id_sql)
            record_db = cr3.fetchone()
            cr3.close()
            if record_db:
                cr1 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                record_db_id = record_db[0]
                get_migrated_field_sql = f"UPDATE {migrated_table} SET {migrated_field} = %s WHERE id = %s;"
                cr1.execute(get_migrated_field_sql, (value, record_db_id))
                cr1.close()
                self.update_migrated_data_history(connection.cursor(), record_id, table)
                self.updatesuccessfullfields()
                self.updatesuccessfullrecords()
        print(str(number) + " records deanonymized!")
    
    def update_migrated_data_history(self, cr, id, table):
        cr.execute(f"UPDATE {constants.TABLE_MIGRATED_DATA}_{table} SET STATE = 1 WHERE ID = {id}")
    
