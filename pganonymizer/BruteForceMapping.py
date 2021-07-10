import psycopg2, sys
from pganonymizer.utils import convert_list_to_sql

#todo in den ausblick schreiben, dass performanteres erstellen des mappings geplant ist


def get_data(check, value):
    for c in check:
        if c[0]+"_"+c[1]+"_" in value:
            return c[0], c[1]
    return False

def remove_not_migrated(fields, con):
    cursor = con.cursor()
    field_to_be_migrated = []
    for field in fields:
        table = field[0].replace(" ","")
        field = field[1].replace(" ","")
        try:
            cursor.execute(f"select exists ( select from {table} where \"{field}\" LIKE '{table}_{field}_%');")
        except:
            field_to_be_migrated.append((table, field))
            continue
        migrated_fields = cursor.fetchone()
        if not migrated_fields[0]:
            field_to_be_migrated.append((table, field))
            continue
        cursor.execute(f"select exists ( select from model_migration_mapping where old_model_name = '{table}' and old_field_name = '{field}');")
        exist = cursor4.fetchone()
        if not exist[0]:
            sql = f"Insert into model_migration_mapping \
                            (old_model_name, new_model_name, old_field_name, new_field_name)\
                             VALUES (%s, %s, %s, %s);"
            data = (table, table, field, field)
            cursor.execute(sql, data)
            cursor.execute('COMMIT;')
    cursor.close()
    return field_to_be_migrated
        
def brute_force_mapping(con, db):
    created_data = []
    cursor = con.cursor()
    cursor2 = con.cursor()
    cursor3 = con.cursor()
    cursor4 = con.cursor()
    cursor.execute(f"select model_id, field_id from migrated_fields;")
    fields_to_check = cursor.fetchall()
    fields_to_check = remove_not_migrated(fields_to_check, con)
    if not fields_to_check:
        return created_data
    cursor.execute(f"select table_name from information_schema.tables where table_type = 'BASE TABLE' and table_schema = 'public' and table_catalog = '{db}';")
    tables = cursor.fetchall()
    cursor2.execute(f" SELECT table_name, column_name FROM information_schema.columns WHERE data_type in ('text', 'character varying') and table_name in {convert_list_to_sql([x[0] for x in tables], char=True)};")
    while True:
        fields = cursor2.fetchmany()
        if not fields:
            break
        for field in fields:
            table = field[0]
            field = field[1]
            cursor3.execute(f"select \"{field}\" from {table} where \"{field}\" ~'.*_.*_[0-9]+$' LIMIT 1;")
            migrated_fields = cursor3.fetchone()
            if not migrated_fields:
                continue
            value = migrated_fields[0]
            result = get_data(fields_to_check,value)
            if not result:
                continue
            old_table = result[0]
            old_field = result[1]
            cursor4.execute(f"select exists ( select from model_migration_mapping where old_model_name = '{old_table}' and old_field_name = '{old_field}');")
            exist = cursor4.fetchone()
            if not exist[0]:
                sql = f"Insert into model_migration_mapping \
                                (old_model_name, new_model_name, old_field_name, new_field_name)\
                                 VALUES (%s, %s, %s, %s);"
                data = (old_table, table, old_field, field)
                cursor4.execute(sql, data)
                cursor.execute('COMMIT;')
                created_data.append((table, field))
    return created_data
                        
                    
