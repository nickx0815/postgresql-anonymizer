import psycopg2, datetime
from pganonymizer.constants import constants

def update_fields_history(cr, model_id, record, state, field_id):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr.execute(f"Insert into ir_model_fields_anonymization_history ( \
                state, model_id, field_to_group,create_date, write_date, create_uid, record_id \
            ) values ( \
                {state}, '{model_id}','{field_id}', '{str(now)}', '{str(now)}', {1}, {record});")
    
def update_migrated_data_history(cr, id):
    cr.execute(f"UPDATE {constants.TABLE_MIGRATED_DATA} SET STATE = 1 WHERE ID = {id}")

