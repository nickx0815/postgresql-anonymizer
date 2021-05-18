import psycopg2
from pganonymizer.update_field_history import update_fields_history


def create_anon_db(connection, data, ids):
    cr = connection.cursor()
    try:
        cr.execute("CREATE TABLE anon_db(\
                         model_id VARCHAR,\
                         field_id VARCHAR,\
                         record_id INTEGER,\
                         value VARCHAR,\
                         PRIMARY KEY (model_id, field_id, record_id));")
        cr.execute("COMMIT;")
    except:
        cr.execute("ROLLBACK;")
    try:
        cr.execute("CREATE TABLE anon_fields_db(\
                         model_id VARCHAR,\
                         field_id VARCHAR,\
                         PRIMARY KEY (model_id, field_id);")
        cr.execute("COMMIT;")
    except:
        cr.execute("ROLLBACK;")
    cr.close()
    _run_query(connection, data, ids)
    
    
def _run_query(con, data, ids):
    for table in data:
        if table == 'anon':
            create_anon(con ,data[table], ids)
        elif table == 'truncate':
            create_truncate(con, data[table])



def create_anon(con ,data, ids):
    cr = con.cursor()
    for table in data:
        table_sql = "Select id FROM ir_model WHERE model = '{model_data}'".format(model_data=_(table))
        cr.execute(table_sql)
        table_id = cr.fetchone()[0]
        for field in data.get(table):
            ids_sql_format = str(set([x for x in ids])).replace("{","(").replace("}",")")
            insert_anon_field_rec(cr, field, table)
            field_sql = "Select id From ir_model_fields_anonymization Where field_name = '{field_name}' AND model_id = {table_id} and id in {tuple_ids}".format(field_name=field,
                                                                                                                                                                table_id=table_id,
                                                                                                                                                                tuple_ids=ids_sql_format)
            cr.execute(field_sql)
            field_id = cr.fetchone()[0]
            for id in data.get(table).get(field):
                sql_anon_db_insert = "Insert into anon_db (model_id, field_id, record_id, value) \
                VALUES ('{model_id}', '{field_id}', {record_id}, '{value}')".format(
                    model_id = table, field_id = field, record_id = id, value = data.get(table).get(field).get(id))
                cr.execute(sql_anon_db_insert)
                update_fields_history(cr, table_id, field_id, id, 2)
    cr.execute("COMMIT;")
    cr.close()

def insert_anon_field_rec(cr, field, table):
        sql_insert = "INSERT INTO anon_fields_db (model_id, field_id) \
                       VALUES ('{table}', '{field}') \
                       WHERE NOT EXISTS ( SELECT * FROM anon_fields_db \
                               WHERE model_id = '{table}' \
                                       AND field_id = '{field}');".format(table=table, field=field)
        cr.execute(sql_insert)
        

def get_field_mappings(connection, args):
    data = {}
    cr = connection.cursor(cursor_factory=psycopg2.extras.DictCursor, name='fetch_large_result')
    get_field_mapping_sql = "select * from field_mapping_version_12".format(table_name=args.anon_table)
    cr.execute(get_field_mapping_sql)
    while True:
        records = cr.fetchmany(size=2000)
        if not records:
            break
        for record in records:
            return
    cr.close()

def run_revert(connection, args):
    field_mappings = get_field_mappings(connection, args)
    cr = connection.cursor(cursor_factory=psycopg2.extras.DictCursor, name='fetch_large_result')
    get_anon_data_sql = "select model_id, field_id, record_id from {table_name} where 1=1".format(table_name=args.anon_table)
    cr.execute(get_anon_data_sql)
    while True:
        records = cr.fetchmany(size=2000)
        if not records:
            break
        for record in records:
            break
    cr.close()

def create_truncate(con, data):
    cr = con.cursor()
#     x = "s"
#     cr.execute("COMMIT;")
    cr.close()
    
def _(t):
    return t.replace("_", ".")
    
    
