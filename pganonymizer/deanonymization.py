import psycopg2
from pganonymizer.utils import update_fields_history
from pganonymizer.constants import constants

def run_revert(connection, args, data):
    for table, data in data.items():
        number = 0
        mapped_field_data = _get_mapped_data(connection, table, field=data[0])
        original_table = mapped_field_data[0]
        migrated_table = mapped_field_data[1]
        original_field = mapped_field_data[2]
        migrated_field = mapped_field_data[3]
        for id, value, record_id in data[1]:
            number = number + 1
            cr3 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
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
                update_fields_history(connection.cursor(cursor_factory=psycopg2.extras.DictCursor), original_table, record_db_id, "4", original_field)
                update_migrated_data_history(connection.cursor(), record_id)
    print(str(number) + " records deanonymized!")

def update_migrated_data_history(cr, id):
    cr.execute(f"UPDATE {constants.TABLE_MIGRATED_DATA} SET STATE = 1 WHERE ID = {id}")

def _get_mapped_data(con, table, field=False):
    # todo function to determine which mapping (10,11,12...)
    cr = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    select_model_id_sql = f"SELECT new_model_name, new_field_name FROM {constants.TABLE_MIGRATED_DATA_MAPPING} where old_model_name = '{table}'"
    if field:
        select_model_id_sql+=f" and old_field_name = '{field}'"
    select_model_id_sql+=";"
    cr.execute(select_model_id_sql)
    record = cr.fetchone()
    if not record:
        return (table, table, field, field)
    if not field:
        return (table, record.get('new_model_name'))
    return (table, record.get('new_model_name'), field, record.get('new_field_name'))
