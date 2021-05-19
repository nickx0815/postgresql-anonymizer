import psycopg2, datetime

def update_fields_history(cr, model_id, record, state, field_id=False, revert_field=False):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if field_id:
        cr.execute("Insert into ir_model_fields_anonymization_history ( \
                state, model_id, field_to_group, field_id, create_date, write_date, create_uid, record_id \
            ) values ( \
                {state}, {model_id},{field_to_group}, {field_id}, '{create_date}', '{write_date}', {create_uid}, {record_id});".format(state = state,
                       model_id = model_id,
                       field_to_group = field_id['field_id'],
                       field_id = field_id['id'],
                       create_date = str(now),
                       write_date = str(now),
                       create_uid=1,
                       record_id=record))
    elif revert_field:
        cr.execute("Insert into ir_model_fields_anonymization_history ( \
                state, model_id, field_to_group, revert_field_id, create_date, write_date, create_uid, record_id \
            ) values ( \
                {state}, {model_id},{field_to_group},{revert_field_id}, '{create_date}', '{write_date}', {create_uid}, {record_id});".format(state = state,
                       model_id = model_id,
                       field_to_group = revert_field,
                       revert_field_id = revert_field,
                       create_date = str(now),
                       write_date = str(now),
                       create_uid=1,
                       record_id=record))
