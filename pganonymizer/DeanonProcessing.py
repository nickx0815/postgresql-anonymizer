import psycopg2
from pganonymizer.utils import update_fields_history, _get_mapped_data
from pganonymizer.constants import constants
from pganonymizer.logging import logger
from pganonymizer.MainProcessing import MainProcessing
logging_ = logger()

class DeanonProcessing(MainProcessing):
    
    type = "deanonymization"
    
    def __init__(self, main_job, tmpconnection, totalrecords, schema, table, pg_args, type):
        super(DeanonProcessing, self).__init__(main_job, totalrecords, schema, table, pg_args, type, main_job.logging_)
        self.tmpcon = tmpconnection
        
    def _get_rel_method(self):
        return "run_revert"

    def run_revert(self, connection):
        data = self.schema
        table = self.table
        number = 0
        mapped_field_data = _get_mapped_data(connection, table, fields=[data[0]])[0]
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
                self.revert_anonymization(connection, record_db, migrated_table, migrated_field, value)
                self.update_migrated_data_history(connection.cursor(), record_id, table)
                self.updatesuccessfullfields()
                self.updatesuccessfullrecords()
        #print(str(number) + " records deanonymized!")
    
    @logging_.UPDATE_MIGRATED_DATA
    def update_migrated_data_history(self, cr, id, table):
        cr.execute(f"UPDATE {constants.TABLE_MIGRATED_DATA}_{table} SET STATE = 1 WHERE ID = {id}")
    
    @logging_.DEANONYMIZATION_RECORD
    def revert_anonymization(self, connection, record, table, field, value):
        cr1 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        record_db_id = record[0]
        get_migrated_field_sql = f"UPDATE {table} SET {field} = %s WHERE id = %s;"
        cr1.execute(get_migrated_field_sql, (value, record_db_id))
        cr1.close()
    
