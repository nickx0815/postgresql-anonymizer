import psycopg2
from pypika import Query, Table, Field



def create_revert_script(data):
    queries = _get_queries(data)
    sql_script = _get_script(queries)
    _write_to_file(sql_script)
    return sql_script
    
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

def _get_script(q):
    return ";".join(q)

def _write_to_file(file):
    with open("script.sql", "w") as sql:
        sql.write(file)
    