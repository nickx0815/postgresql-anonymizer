# Database arguments used for the CLI
DATABASE_ARGS = ('dbname', 'user', 'password', 'host', 'port')

# Default name for the primary key column
DEFAULT_PRIMARY_KEY = 'id'

# Delimiter used to buffer and import database data.
COPY_DB_DELIMITER = '\x1f'

# Filename of the default schema
DEFAULT_SCHEMA_FILE = 'schema.yml'

section = ['Required','Optional']
testarg = ['schema', 'dbname', 'user', 'password', 'host', 'port']
testarg_optional = ['dump', 'dry-run', 'l', 'v']

NUMBER_MAX_THREADS = 4
#TODO use other technique
# available arg ['-v', '-l', '--dry-run']
