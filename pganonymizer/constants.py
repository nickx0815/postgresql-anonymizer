# Database arguments used for the CLI

class constants():

    DATABASE_ARGS = ('dbname', 'user', 'password', 'host', 'port')
    
    # Default name for the primary key column
    DEFAULT_PRIMARY_KEY = 'id'
    
    # Delimiter used to buffer and import database data.
    COPY_DB_DELIMITER = '\x1f'
    
    # Filename of the default schema
    DEFAULT_SCHEMA_FILE = 'schema.yml'
    
    PATH_SCHEMA_FILES =  "/home/migration/schema/"
    PATH_CONFIG_FILE = '/home/migration/migrationConfig.conf'
    TABLE_MIGRATED_DATA = 'migrated_data'
    TABLE_MIGRATED_FIELDS = 'migrated_fields'
    PATH_DUMP =  "/tmp/"
    
    section = ['Required','Optional']
    testarg = ['schema', 'dbname', 'user', 'password', 'host', 'port', 'type']
    testarg_optional = ['dump', 'dry_run', 'l', 'v', 'schema', 'threading', 'force_path']
    
    NUMBER_MAX_THREADS_ANON = 8
    NUMBER_MAX_THREADS_DEANON = 8
    
    
    DEANON_NUMBER_FIELD_PER_THREAD = 1000
    ANON_NUMBER_FIELD_PER_THREAD = 2500
    ANON_FETCH_RECORDS = 2500
    #TODO use other technique
    # available arg ['-v', '-l', '--dry-run']
