import sys, time
import psycopg2
no_pretty_table = False
try:
    from prettytable import PrettyTable
except:
    no_pretty_table = True

def _get_ids_sql_format(ids, char=False):
    if ids:
        parsed =  str(set([x for x in ids])).replace("{", "(").replace("}", ")")
        if not char:
            return parsed.replace("'","")
        return parsed
    return False

def run_analyse(con, db, tables=False):
    cursor = con.cursor()
    cursor2 = con.cursor()
    if not tables:
        orig_tables = "like '%'"
    else:
        orig_tables = f"in {_get_ids_sql_format(tables, char=True)}"
    cursor.execute(f"select table_name from information_schema.tables where table_name {orig_tables} and table_type = 'BASE TABLE' and table_schema = 'public' and table_catalog = '{db}';")
    orig_tables = cursor.fetchall()
    for table in orig_tables:
        anonymizable_fields = []
        anonymized_fields = []
        non_anonymized_fields = []
        info_table = table[0]
        #TODO suche muss angepasst werden, es werden felder gefunden welche bei der suche auf der tabelle dann nicht exisiieren
        # muss schauen wie ich die where clause anpassen muss
        cursor.execute(f" SELECT column_name FROM information_schema.columns WHERE data_type in ('text', 'character varying') and table_name = '{info_table}';")
        while True:
            field_name = cursor.fetchall()
            if not field_name:
                break
            if not no_pretty_table:
                x = PrettyTable()
                x.field_names = ["Fieldname", "Percent Anonymized", "Total Records", "Total Anonymized Records"]
            else:
                x = "Fieldname    Percent Anonymized    Total Records    Total Anonymized Records\n"
            for field in field_name:
                anonymizable_fields.append(field)
                field = field[0]
                cursor2.execute(f"select exists (select * from {info_table} where {field} like '{info_table}_{field}%');")
                result = cursor2.fetchone()
                if not result[0]:
                    non_anonymized_fields.append(field)
                    continue
                if result[0]:
                    anonymized_fields.append(field)
                    cursor.execute(f"select count(*) from {info_table} where {field} is not NULL and {field} <> '';")
                    total_number = cursor.fetchone()[0]
                    cursor.execute(f"select count(*) from {info_table} where {field} like '{info_table}_{field}%' \
                        and {field} is not NULL and {field} <> '';")
                    anonymized_number = cursor.fetchone()[0]
                    percent = round(anonymized_number/total_number*100,2) if total_number != 0 else 0.0
                    if not no_pretty_table:
                        x.add_row([field, percent, total_number, anonymized_number])
                    else:
                        x = x + f"{field}, {percent} %, {total_number}, {anonymized_number}\n"
            if len(anonymized_fields) != 0:
                print(f"{info_table}")
                print(x)
                print(f"number fields {len(anonymized_fields)}")
                print("")
    print("finished")


