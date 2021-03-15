import psycopg2
from pypika import Query, Table, Field



def create_revert_script(connection, data):
    cr = connection.cursor()
    cr.execute("CREATE TABLE anon_db(\
                         model_id VARCHAR,\
                         field_id VARCHAR,\
                         record_id INTEGER,\
                         value VARCHAR);")
    cr.execute("CREATE INDEX model_id_idx ON anon_db (model_id);")
    cr.execute("CREATE INDEX field_id_idx ON anon_db (field_id);")
    cr.execute("COMMIT;")
    
    _run_query(cr, data)
    
def _run_query(cr, data):
    table_ir_model = Table('ir_model')
    table_ir_model_fields = Table('ir_model_fields')
    table_anon_db = Table('anon_db')
    for table in data:
        table_sql = str(Query.from_(table_ir_model).select("id").where(table_ir_model.model == _(table)))
        cr.execute(table_sql)
        table_id = cr.fetchone()[0]
        for field in data.get(table):
            field_sql = str(Query.from_(table_ir_model_fields).select('id').where(table_ir_model_fields.name == field).where(table_ir_model_fields.model_id == table_id))
            cr.execute(field_sql)
            field_id = cr.fetchone()[0]
            for id in data.get(table).get(field):
                sql_anon_db_insert = str(Query.into(table_anon_db).insert(table_id, field_id, id, data.get(table).get(field).get(id)))
                cr.execute(sql_anon_db_insert)
    cr.execute("COMMIT;")
    
def _(t):
    return t.replace("_", ".")

def _get_queries(d):
    q = []
    for table_name in d:
        data_table = d.get(table_name)
        for id in data_table:
            field_datas = data_table.get(id)
            for field_data in field_datas:
                table = Table(table_name)
                q.append(str(Query.update(table).set(field_data, field_datas.get(field_data)).where(table.id ==id)))
    return q
    
    