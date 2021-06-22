import psycopg2, logging, csv
from pganonymizer.update_field_history import update_fields_history, update_migrated_data_history
from docutils.nodes import row
from pganonymizer.constants import constants


def _run_query(type, con, data, ids, table_id):
    if type == 'anon':
        create_anon(con , data, ids, table_id)
    elif type == 'truncate':
        create_truncate(con, data)


def _get_ids_sql_format(ids):
    if ids:
        return str(set([x for x in ids])).replace("{", "(").replace("}", ")")
    return False

def create_anon(con, data, ids, table_id):
    cr = con.cursor()
    for table, field_data in data.items():
        # ids_sql_format = _get_ids_sql_format(ids)
        field = list(field_data.keys())[0]
        insert_migrated_fields_rec(cr, field, table)
        id = data.get(table).get(field)
        sql_migrated_data_insert = f"Insert into {constants.TABLE_MIGRATED_DATA} (model_id, field_id, record_id, value, state) VALUES (%s, %s, %s, %s, %s)"
        id = list(id.keys())[0]
        data = (table, field, id, data.get(table).get(field).get(id), 0)
        cr.execute(sql_migrated_data_insert, data)
        update_fields_history(cr, table, id, "2", field)
    cr.close()

def insert_migrated_fields_rec(cr, field, table):
    sql_insert = f"INSERT INTO {constants.TABLE_MIGRATED_FIELDS} (model_id, field_id) VALUES ('{table}', '{field}');"
    sql_select = f"SELECT id  from {constants.TABLE_MIGRATED_FIELDS} \
                            WHERE model_id = '{table}' \
                                   AND field_id = '{field}' \
                            LIMIT 1;"
    cr.execute(sql_select)
    record = cr.fetchone()
    if not record:
        cr.execute(sql_insert)
        
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

def _get_mapped_data(con, table, field=False):
    # todo function to determine which mapping (10,11,12...)
    cr = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    select_model_id_sql = f"SELECT new_model_name, new_field_name FROM model_migration_mapping where old_model_name = '{table}'"
    if field:
        select_model_id_sql+=f" and old_field_name = '{field}'"
    select_model_id_sql+=";"
    cr.execute(select_model_id_sql)
    record = cr.fetchone()
    if not record:
        return (table, table, field, field)
    return (table, record.get('new_model_name'), field, record.get('new_field_name'))

def create_truncate(con, data):
    cr = con.cursor()
    cr.close()
    
def _(t):
    return t.replace("_", ".")
    
