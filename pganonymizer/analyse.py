import sys, time
import psycopg2
no_pretty_table = False
try:
    from prettytable import PrettyTable
except:
    no_pretty_table = True

def run_analyse(con):
    cursor = con.cursor()
    cursor2 = con.cursor()
    orig_tables = ["%"]
    for table in orig_tables:
        cursor.execute(f"select table_name from information_schema.tables where table_name like '{table}';")
        info_tables = cursor.fetchall()
        if not info_tables:
            print(f"no table found for {table}")
            continue
        for info_table in info_tables:
            anonymizable_fields = []
            anonymized_fields = []
            non_anonymized_fields = []
            info_table = info_table[0]
            cursor.execute(f"select name from ir_model_fields where model = '{info_table.replace('_','.')}' and store = true and ttype in ('char', \
                                     'text', \
                                     'html',\
                                     'selection');")
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
                    try:
                        cursor2.execute(f"select exists (select * from {info_table} where {field} like '{info_table}_{field}%');")
                    except:
                        non_anonymized_fields.append(field)
                        continue
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
                            x.append(f"{field}, {percent} %, {total_number}, {anonymized_number}\n")
                if len(anonymized_fields) != 0:
                    print(f"{info_table}")
                    print(x)
                    print(f"number fields {len(anonymized_fields)}")
                    print("")
    print("finished")


