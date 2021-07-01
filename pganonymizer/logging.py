import logging, datetime
from pganonymizer.constants import constants


class logger():
    
    def get_default_log_level(self):
        return logging.DEBUG
    
    def get_config_parameter(self):
        
        args = {'format':'%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    'datefmt':'%d-%m-%y %H:%M:%S',
                    'filename': constants.PATH_LOG_FILES,
                   'filemode':'w'}
        return args
    
    def __init__(self):
        logging.basicConfig(**self.get_config_parameter())
        logger = logging.getLogger(__name__)
        logger.setLevel(self.get_default_log_level())
        self.logger_ = logger
        
    def setLogLevel(self, args):
        level = getattr(logging, args.logging)
        self.logger_.setLevel(level)
    
    def TEST_CONNECTION(self, function):
        def test_connection(self):
            result = function(self)
            self.logger.logger_.debug(f'the connection was set up successfully')
            self.logger.logger_.debug(f'connection data {self.pg_args}')
            return result
        return test_connection
    
    def GET_SCHEMA(self, function):
        def get_schema(self, args):
            if args.force_path_schema:
                self.logger.logger_.debug(f"the default schema path was forced to {args.force_path_schema}")
            result = function(self, args)
            self.logger.logger_.debug(f'the schema was loaded successfully')
            self.logger.logger_.debug(f'schema data {self.schema}')
            return result
        return get_schema
    
    def NUMBER_THREAD(self, function):
        def get_thread_number(self):
            result = function(self)
            self.logger.logger_.debug(f"Number of threads created: {result}")
            return result
        return get_thread_number
    
    def THREAD_STARTED(self, function):
        def start_thread(self, job):
            self.logger.logger_.debug(f"Thread started")
            self.logger.logger_.debug(f'job\'s data {job}')
            result = function(self, job)
            self.logger.logger_.debug(f"Thread finished")
            return result
        return start_thread
    
    def RESULTS(self, function):
        def start(self):
            result = function(self)
            runtime = str(datetime.timedelta(seconds=self.endtime-self.starttime))
            main = f"the {self.type_to_method_mapper(self.type)} of {self.table} took {runtime}\n"
            additionalrecordsinfo = f"successfull processed {self.successfullrecords} (total records {self.totalrecords})\n"
            additionalfieldsinfo = f"successfull processed {self.successfullfields} fields\n"
            self.logger.logger_.debug(main)
            self.logger.logger_.debug(additionalrecordsinfo)
            self.logger.logger_.debug(additionalfieldsinfo)
            return result
        return start
    
    def ANONYMIZATION_RECORD(self, function):
        def import_data(self, connection, field, source_table, row_id, primary_key, value):
            result = function(self, connection, field, source_table, row_id, primary_key, value)
            self.logger.logger_.info(f'{source_table} {row_id} {field} anonymized -> {value}')
            return result
        return import_data
    
    def EXCLUDE_RECORD(self, function):
        def row_matches_excludes(self, row, excludes=None):
            result = function(self, row, excludes=None)
            if result:
                self.logger.logger_.debug(f'{self.table} {row.get("id")} excluded')
            return result
        return row_matches_excludes
    
    def TRUNCATE_TABLES(self, function):
        def truncate_tables(self, connection):
            result = function(self, connection)
            self.logger.logger_.info(f'{self.schema} deleted')
            return result
        return truncate_tables
    
    def INSERT_MIGRATED_FIELD(self, function):
        def insert_migrated_fields_rec(self, cr, field, table):
            result = function(self, cr, field, table)
            self.logger.logger_.debug(f'INSERT INTO {table} ({field})')
            return result
        return insert_migrated_fields_rec
    
    def INSERT_MIGRATED_DATA(self, function):
        def create_anon(self, con, table, data):
            result = function(self, con, table, data)
            migrated_table = constants.TABLE_MIGRATED_DATA+"_"+table
            field = list(data.keys())[0]
            id = list(data.get(field).keys())[0]
            value = data.get(field).get(id)
            self.logger.logger_.info(f'{migrated_table} new({field} {id} {value})')
            return result
        return create_anon
    
    def UPDATE_MIGRATED_DATA(self, function):
        def update_migrated_data_history(self, cr, id, table):
            result = function(self, cr, id, table)
            migrated_table = constants.TABLE_MIGRATED_DATA+"_"+table
            self.logger.logger_.debug(f'UPDATE {migrated_table} (state:1, id:{id})')
            return result
        return update_migrated_data_history
    
    def DEANONYMIZATION_RECORD(self, function):
        def revert_anonymization(self, connection, record, table, field, value):
            result = function(self, connection, record, table, field, value)
            self.logger.logger_.info(f'{table} {record[0]} {field} deanonymized -> {value}')
            return result
        return revert_anonymization
    
    
        

            
