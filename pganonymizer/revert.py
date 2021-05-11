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
                update_fields_history(cr, table_id, field_id, id)
    cr.execute("COMMIT;")
    cr.close()

def create_truncate(con, data):
    cr = con.cursor()
    x = "s"
    cr.execute("COMMIT;")
    cr.close()
    
def _(t):
    return t.replace("_", ".")
    
    