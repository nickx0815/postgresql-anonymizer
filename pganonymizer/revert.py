import psycopg2
from pganonymizer.update_field_history import update_fields_history
import logging


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
        cr.execute("CREATE TABLE anon_field_db(\
                         model_id VARCHAR,\
                         field_id VARCHAR,\
                         PRIMARY KEY (model_id, field_id));")
        cr.execute("COMMIT;")
    except:
        cr.execute("ROLLBACK;")
    cr.close()
    
    
def _run_query(type, con, data, ids):
    create_anon_db(con, data, ids)
    if type == 'anon':
        create_anon(con ,data, ids)
    elif type == 'truncate':
        create_truncate(con, data)

def _get_ids_sql_format(ids):
    return str(set([x for x in ids])).replace("{","(").replace("}",")")


def create_anon(con, data, ids):
    cr = con.cursor()
    for table in data:
        table_sql = "Select id FROM ir_model WHERE model = '{model_data}'".format(model_data=_(table))
        cr.execute(table_sql)
        table_id = cr.fetchone()[0]
        for field in data.get(table):
            ids_sql_format = _get_ids_sql_format(ids)
            insert_anon_field_db_rec(cr, field, table)
            field_sql = "Select id, field_id From ir_model_fields_anonymization Where field_name = '{field_name}' AND model_id = {table_id} and id in {tuple_ids}".format(field_name=field,
                                                                                                                                                                table_id=table_id,
                                                                                                                                                                tuple_ids=ids_sql_format)
            cr.execute(field_sql)
            field_id = cr.fetchone()
            for id in data.get(table).get(field):
                sql_anon_db_insert = "Insert into anon_db (model_id, field_id, record_id, value) \
                VALUES ('{model_id}', '{field_id}', {record_id}, '{value}')".format(
                    model_id = table, field_id = field, record_id = id, value = data.get(table).get(field).get(id))
                cr.execute(sql_anon_db_insert)
                update_fields_history(cr, table_id, id, "2", field_id = field_id)
    cr.execute("COMMIT;")
    cr.close()

def insert_anon_field_db_rec(cr, field, table):
    sql_insert = "INSERT INTO anon_field_db (model_id, field_id) \
                   VALUES ('{table}', '{field}');".format(table=table, field=field)
    sql_select = "SELECT *  from anon_field_db \
                            WHERE model_id = '{table}' \
                                   AND field_id = '{field}';".format(table=table, field=field)
    cr.execute(sql_select)
    record = cr.fetchone()
    if not record:
        cr.execute(sql_insert)
        
def get_anon_fields(connection, args, ids=None):
    data = {}
    cr = connection.cursor(cursor_factory=psycopg2.extras.DictCursor, name='fetch_large_result')
    if ids:
        where_clause = "WHERE ID IN {ids}".format(ids = _get_ids_sql_format(ids))
    else:
        where_clause = " "
    logging.info("here is the where clause    "+where_clause)
    get_anon_fields = "SELECT * FROM anon_field_db {WHERE};".format(WHERE=where_clause)
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

def run_revert(connection, args, ids=None):
    anon_fields = get_anon_fields(connection, args, ids=ids)
    cr1 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cr2 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for table, fields in anon_fields.items():
        mapped_table, mapped_fields = get_mapped_field_data(connection, table, fields)
        original_table = mapped_table[0]
        migrated_table = mapped_table[1]
        for mapped_field in mapped_fields:
            original_field = mapped_field[0]
            migrated_field = mapped_field[1]
            migrated_model_id, migrated_field_id = get_db_ids(connection, migrated_table, migrated_field)
            get_anon_data_sql = "SELECT * FROM {table_name} where model_id = '{original_table}' and field_id = '{original_field}';".format(table_name=args.anon_table,
                                                                                                                                           original_table = original_table,
                                                                                                                                           original_field = original_field)
            cr1.execute(get_anon_data_sql)
            while True:
                records = cr1.fetchmany(size=2000)
                if not records:
                    break
                for record in records:
                    logging.info(" yea yea yea")
                    cr3 = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                    value = original_table+"_"+original_field+"_"+str(record['record_id'])
                    record_db_id_sql = "SELECT ID FROM {mapped_table} where {mapped_field} = '{value}';".format(
                        mapped_table=migrated_table,
                        mapped_field=migrated_field,
                        value=value)
                    cr3.execute(record_db_id_sql)
                    record_db = cr3.fetchone()
                    if record_db:
                        record_db_id = record_db[0]
                    #need id of record that are updated
                        get_migrated_field_sql = "UPDATE {mapped_table} SET {mapped_field} = '{original_value}' WHERE  id = {rec_id};".format(mapped_table=migrated_table,
                                                                                                                                                        mapped_field=migrated_field,
                                                                                                                                                        original_value=record['value'],
                                                                                                                                                        rec_id = record_db_id)
                        cr2.execute(get_migrated_field_sql)
                        update_fields_history(cr2, migrated_model_id, record_db_id, "4", revert_field = migrated_field_id)
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
    return model_id, field_id

def get_mapped_field_data(connection, table, fields):
    cr = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    list_field_mapped = []
    migration_table = False
    for field in fields:
        sql_select = "SELECT *  from ir_model_fields_anonymization_migration_fix \
                                WHERE model_id = '{table}' \
                                       AND field_id = '{field}';".format(table=table, field=field)
        cr.execute(sql_select)
        record = cr.fetchone()
        if record and record.get('migration_field'):
            migration_field = record.get('migration_field')
        else:
            migration_field = field
        if record and record.get('migration_model') and not migration_table:
            migration_table = record.get('migration_model')
        # insert mapping functionality
        #[(field, mapped_field), (field, mapped_field)...]
        list_field_mapped.append((field, migration_field))
    if not migration_table:
        migration_table = table
    cr.close()
    migrated_table = (table, migration_table)
    return migrated_table, list_field_mapped

def create_truncate(con, data):
    cr = con.cursor()
#     x = "s"
#     cr.execute("COMMIT;")
    cr.close()
    
def _(t):
    return t.replace("_", ".")
    
    
