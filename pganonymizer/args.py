class Args():
    def __init__(self, dic):
        self.verbose = dic.get("v")
        self.list_providers = dic.get("l")
        self.schema = dic.get("schema")
        self.dbname = dic.get("dbname")
        self.user = dic.get("user", "odoo")
        self.password = dic.get("password", "odoo")
        self.host = dic.get("host", "postgres")
        self.port = dic.get("port", 5432)
        self.dry_run = dic.get("dry_run")
        #self.dump_file = dic.get("dump")
        self.threading = dic.get('threading', True)
        self.force_path_schema = dic.get('force_path_schema')
        self.logging = dic.get('logging', 'INFO')
        self.migration = dic.get('migration')
        self.type = dic.get('type')
        self.force_thread_number = dic.get('force_thread_number')
        self.FORCE_ANON_NUMBER_FIELD_PER_THREAD = dic.get('FORCE_ANON_NUMBER_FIELD_PER_THREAD')
        self.FORCE_ANON_FETCH_RECORDS = dic.get('FORCE_ANON_FETCH_RECORDS')