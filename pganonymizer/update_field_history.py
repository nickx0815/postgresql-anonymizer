import psycopg2, datetime

def update_fields_history(cr, model_id, record, state, field_id):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cr.execute("Insert into ir_model_fields_anonymization_history ( \
                state, model_id, field_to_group,create_date, write_date, create_uid, record_id \
            ) values ( \
                {state}, '{model_id}','{field_to_group}', '{create_date}', '{write_date}', {create_uid}, {record_id});".format(state = state,
                       model_id = model_id,
                       field_to_group = field_id,
                       create_date = str(now),
                       write_date = str(now),
                       create_uid=1,
                       record_id=record))
    cr.execute("commit;")

