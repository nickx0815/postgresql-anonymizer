import psycopg2, logging, csv
from pganonymizer.update_field_history import update_fields_history
from docutils.nodes import row

def _run_query(type, con, data, ids, table_id):
    if type == 'anon':
        create_anon(con ,data, ids, table_id)
    elif type == 'truncate':
        create_truncate(con, data)

def _get_ids_sql_format(ids):
    if ids:
        return str(set([x for x in ids])).replace("{","(").replace("}",")")
    return False

def create_anon(con, data, ids, table_id):
    cr = con.cursor()
    for table, field_data in data.items():
        #ids_sql_format = _get_ids_sql_format(ids)
        field = list(field_data.keys())[0]
        insert_migrated_fields_rec(cr, field, table)
        id = data.get(table).get(field)
        sql_migrated_data_insert = "Insert into migrated_data (model_id, field_id, record_id, value) \
            VALUES (%s, %s, %s, %s)"
        id = list(id.keys())[0]
        data = (table, field, id, data.get(table).get(field).get(id))
        cr.execute(sql_migrated_data_insert, data)
        update_fields_history(cr, table, id, "2", field)
    cr.execute("COMMIT;")
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
        
def get_anon_fields(connection, args, ids=None, where_clause=""):
    data = {}
    cr = connection.cursor(cursor_factory=psycopg2.extras.DictCursor, name='fetch_large_result')
    if ids:
        where_clause = "WHERE ID IN {ids}".format(ids = _get_ids_sql_format(ids))
    get_anon_fields = "SELECT * FROM migrated_fields {WHERE};".format(WHERE=where_clause)
    cr.execute(get_anon_fields)
    while True:
        records = cr.fetchmany(size=2000)
        if not records:
            break
        for record in records:
            model = record['model_id']
            field = record['field_id']
            if not data.get(model):
                data[model] = []
            data[model].append(field)
    cr.close()
    return data

def run_revert(connection, args, data):
    cr1 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cr2 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for table, data in data.items():
        mapped_field_data = get_mapped_field_data(connection, table, data[0])
        get_anon_data_sql = "SELECT * FROM {anon_table} where id in {ids};".format(anon_table=args.anon_table,ids = _get_ids_sql_format(data[1]))
        cr1.execute(get_anon_data_sql)
        while True:
            records = cr1.fetchmany(size=2000)
            logging.info("record searched")
            if not records:
                break
            for record in records:
                original_table = mapped_field_data[0]
                migrated_table = mapped_field_data[1]
                original_field = mapped_field_data[2]
                migrated_field = mapped_field_data[3]
                migrated_model_id, migrated_field_id = get_db_ids(connection, migrated_table, migrated_field)
                cr3 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                value = original_table+"_"+original_field+"_"+str(record['record_id'])
                record_db_id_sql = "SELECT ID FROM {mapped_table} where {mapped_field} = '{value}';".format(
                    mapped_table=migrated_table,
                    mapped_field=migrated_field,
                    value=value)
                logging.info("record identified")
                cr3.execute(record_db_id_sql)
                record_db = cr3.fetchone()
                if record_db:
                    record_db_id = record_db[0]
                    get_migrated_field_sql = "UPDATE {mapped_table} SET {mapped_field} = '{original_value}' WHERE  id = {rec_id};".format(mapped_table=migrated_table,
                                                                                                                                                    mapped_field=migrated_field,
                                                                                                                                                    original_value=record['value'],
                                                                                                                                                    rec_id = record_db_id)
                    logging.info("record updated")
                    cr2.execute(get_migrated_field_sql)
                    update_fields_history(cr2, original_table, record_db_id, "4", original_field)
                    cr2.execute("COMMIT;")
                cr3.close()
    
    cr2.close()
    cr1.close()

def get_db_ids(connection, mapped_table, mapped_field):
    cr = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    select_model_id_sql = "SELECT id FROM ir_model where model ='{mapped_table}';".format(mapped_table=mapped_table.replace("_","."))
    cr.execute(select_model_id_sql)
    model_id = cr.fetchone()[0]
    select_field_id_sql = "select id from ir_model_fields where model = '{table_name}' and name = '{field_name}';;".format(table_name=mapped_table.replace("_","."),
                                                                                                                          field_name=mapped_field)
    cr.execute(select_field_id_sql)
    field_id = cr.fetchone()[0]
    cr.close()
    return model_id, field_id

def _get_mapped_data(con, table):
    #todo function to determine which mapping (10,11,12...)
    cr = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    data = []
    select_model_id_sql = "SELECT old_model_name, new_model_name, old_field_name, new_field_name FROM model_migration_mapping where old_model_name = '{old_table}';".format(old_table=table)
    cr.execute(select_model_id_sql)
    while True:
        records = cr.fetchmany(size=2000)
        if not records:
            break
        for row in records:
            data.append((row.get('old_model_name'), row.get('new_model_name'),  row.get('old_field_name'), row.get('new_field_name')))
    return data

def mapping_exists(table, field, mapped_data):
    for mapping_data in mapped_data:
        if mapping_data[2] == field:
            return mapping_data
    return (table, table, field, field)

def get_mapped_field_data(connection, table, field):
    list_field_data_mapped = []
    mapped_data = _get_mapped_data(connection, table)
    mapped_data = mapping_exists(table, field, mapped_data)
    list_field_data_mapped.append(mapped_data)
    return list_field_data_mapped

def create_truncate(con, data):
    cr = con.cursor()
    cr.close()
    
def _(t):
    return t.replace("_", ".")
    
