import psycopg2, datetime

def update_fields_history(con, model_id, field_id):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    con.execute("""\
        Insert into ir_model_fields_anonymization_history (
            state, model_id, field_id, create_date, write_date, create_uid
        ) values (
            {state}, {model_id},{field_id}, '{create_date}', '{write_date}', {create_uid});
        """.format(state = 2,
                   model_id = model_id,
                   field_id = field_id,
                   create_date = str(now),
                   write_date = str(now),
                   create_uid=1))
