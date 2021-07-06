# Database arguments used for the CLI
from datetime import datetime

date = datetime.now().strftime("%d_%m_%Y %H:%M:%S")

class constants():

    DATABASE_ARGS = ('dbname', 'user', 'password', 'host', 'port')
    
    # Default name for the primary key column
    DEFAULT_PRIMARY_KEY = 'id'
    
    # Delimiter used to buffer and import database data.
    COPY_DB_DELIMITER = '\x1f'
    
    # Filename of the default schema
    DEFAULT_SCHEMA_FILE = 'schema.yml'
    
    STARTINGUPERROR = "the database system is starting up"
    
    PATH_SCHEMA_FILES =  "/home/migration/schema/"
    PATH_LOG_FILES = f"/home/migration/log/{date}.log"
    #PATH_LOG_FILES = f"/home/inter/Schreibtisch/{date}.log"
    PATH_CONFIG_FILE = '/home/migration/migrationConfig.conf'
    TABLE_MIGRATED_DATA = 'migrated_data_'
    TABLE_MIGRATED_FIELDS = 'migrated_fields'
    TABLE_MIGRATED_DATA_MAPPING = "model_migration_mapping"
    PATH_DUMP =  "/tmp/"
    
    BASIC_TABLES = [TABLE_MIGRATED_FIELDS,TABLE_MIGRATED_DATA_MAPPING]
    
    TABLE_MIGRATED_DEFINITON = {TABLE_MIGRATED_DATA: ["id  SERIAL NOT NULL primary key",
                                                      "field_id CHAR(50)",
                                                      "record_id INTEGER",
                                                      "value VARCHAR",
                                                      "state INTEGER"],
                                TABLE_MIGRATED_FIELDS: ["id  SERIAL NOT NULL primary key",
                                                      "model_id CHAR(50)",
                                                      "field_id CHAR(50)"],
                                TABLE_MIGRATED_DATA_MAPPING: ["id  SERIAL NOT NULL primary key",
                                                      "old_model_name CHAR(50)",
                                                      "new_model_name CHAR(50)",
                                                      "old_field_name CHAR(50)",
                                                      "new_field_name CHAR(50)"],
                                }
    
    section = ['Required','Optional']
    testarg = ['schema', 'dbname', 'user', 'password', 'host', 'port', 'type', 'migration']
    testarg_optional = ['dry_run', 'l', 'v', 'schema', 'threading', 'force_path_schema', 'force_thread_number',
                        'logging', 'FORCE_ANON_NUMBER_FIELD_PER_THREAD', 'FORCE_ANON_FETCH_RECORDS']
    PROCESS_METHOD_MAPPING = {'anonymization': 'anonymize_tables',
                              'truncate':'truncate_tables'}
    
    
    
    DEFAULT_NUMBER_THREADS = 8
    
    
    DEANON_NUMBER_FIELD_PER_THREAD = 1000
    ANON_NUMBER_FIELD_PER_THREAD = 2500
    ANON_FETCH_RECORDS = 2500
    #TODO use other technique
    # available arg ['-v', '-l', '--dry-run']

