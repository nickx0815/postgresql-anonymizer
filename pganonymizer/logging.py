import logging

class logger():
    
    def get_default_log_level(self):
        return logging.DEBUG
    
    def get_config_parameter(self):
        args = {'format':'%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    'datefmt':'%m-%d %H:%M',
                    'filename':'/temp/myapp.log',
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
            self.logger.logger_.info(f'the connection was set up successfully')
            self.logger.logger_.debug(f'connection data {self.pg_args}')
            return result
        return test_connection
    
    def GET_SCHEMA(self, function):
        def get_schema(self, args):
            if args.force_path:
                logging.debug(f"the default schema path was forced to {args.force_path}")
            result = function(self, args)
            logging.info(f'the schema was loaded successfullly')
            logging.debug(f'schema data {self.schema}')
            return result
        return get_schema
    
    def NUMBER_THREAD(self, function):
        def get_thread_number(self):
            result = function(self)
            logging.info(f"Number of threads created: {result}")
            return result
        return get_thread_number

            
