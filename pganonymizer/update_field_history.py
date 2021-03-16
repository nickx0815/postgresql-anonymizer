import psycopg2, datetime
from pypika import Query, Table, Field

def update_fields_history(con, model_id, field_id):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    con.execute("""\
        Insert into ir_model_fields_anonymization_history (
            state, field_id, create_date, write_date, create_uid
        ) values (
            {state}, {field_id}, '{create_date}', '{write_date}', {create_uid});
        """.format(state = 2,
                   field_id = field_id,
                   create_date = str(now),
                   write_date = str(now),
                   create_uid=1))
