import psycopg2, logging, csv
from pganonymizer.update_field_history import update_fields_history
from docutils.nodes import row


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
        sql_migrated_data_insert = "Insert into migrated_data (model_id, field_id, record_id, value) \
            VALUES (%s, %s, %s, %s)"
        id = list(id.keys())[0]
        data = (table, field, id, data.get(table).get(field).get(id))
        cr.execute(sql_migrated_data_insert, data)
        update_fields_history(cr, table, id, "2", field)
    cr.execute("commit;")
    cr.close()

def insert_migrated_fields_rec(cr, field, table):
    sql_insert = "INSERT INTO migrated_fields (model_id, field_id) \
                   VALUES ('{table}', '{field}');".format(table=table, field=field)
    sql_select = "SELECT id  from migrated_fields \
                            WHERE model_id = '{table}' \
                                   AND field_id = '{field}' \
                            LIMIT 1;".format(table=table, field=field)
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
        for id, value in data[1]:
            number = number + 1
            cr3 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            orig_value = original_table + "_" + original_field + "_" + str(id)
            record_db_id_sql = "SELECT ID FROM {mapped_table} where {mapped_field} = '{value}';".format(
                mapped_table="tmp_"+migrated_table,
                mapped_field=migrated_field,
                value=orig_value)
            cr3.execute(record_db_id_sql)
            record_db = cr3.fetchone()
            cr3.close()
            if record_db:
                cr1 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                record_db_id = record_db[0]
                get_migrated_field_sql = "UPDATE {migrated_table} SET {migrated_field} = %s WHERE id = %s;".format(migrated_table=migrated_table,
                                                                                                                   migrated_field=migrated_field)
                cr1.execute(get_migrated_field_sql, (value, record_db_id))
                cr1.execute("commit;")
                cr1.close()
                update_fields_history(connection.cursor(cursor_factory=psycopg2.extras.DictCursor), original_table, record_db_id, "4", original_field)
    print(str(number) + " records deanonymized!")

def _get_mapped_data(con, table, field=False):
    # todo function to determine which mapping (10,11,12...)
    cr = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    select_model_id_sql = "SELECT new_model_name, new_field_name FROM model_migration_mapping where old_model_name = '{old_table}'".format(old_table=table)
    if field:
        select_model_id_sql+=" old_field_name = '{field}'".format(field=field)
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
    
