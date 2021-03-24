import psycopg2
#from pypika import Query, Table, Field
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
    _run_query(cr, data, ids)
    
def _run_query(cr, data, ids):
#     table_ir_model = Table('ir_model')
#     table_ir_model_fields = Table('ir_model_fields_anonymization')
#     table_anon_db = Table('anon_db')
    for table in data:
        table_sql = "Select id FROM ir_model WHERE model = '{model_data}'".format(model_data=_(table))
        #table_sql = str(Query.from_(table_ir_model).select("id").where(table_ir_model.model == _(table)))
        cr.execute(table_sql)
        table_id = cr.fetchone()[0]
        for field in data.get(table):
            field_sql = "Select id From ir_model_fields_anonymization Where field_name = '{field_name}' AND model_id = {table_id} and id in {tuple_ids}".format(field_name=field,
                                                                                                                                                                table_id=table_id,
                                                                                                                                                                ids=tuple(ids))
            #field_sql = str(Query.from_(table_ir_model_fields).select('id').where(table_ir_model_fields.field_name == field).where(table_ir_model_fields.model_id == table_id).where(table_ir_model_fields.id.isin(ids)))
            cr.execute(field_sql)
            field_id = cr.fetchone()[0]
            try:
                for id in data.get(table).get(field):
                    sql_anon_db_insert = "Insert into anon_db (model_id, field_id, record_id, value) \
                    VALUES ({model_id}, {field_id}, {record_id}, '{value}')".format(
                        model_id = table_id, field_id = field_id, id = id, value = data.get(table).get(field).get(id))
                    #sql_anon_db_insert = str(Query.into(table_anon_db).insert(table_id, field_id, id, data.get(table).get(field).get(id)))
                    cr.execute(sql_anon_db_insert)
                update_fields_history(cr, table_id, field_id)
                cr.execute("COMMIT;")
            except:
                cr.execute("ROLLBACK;")
    
def _(t):
    return t.replace("_", ".")
    
    