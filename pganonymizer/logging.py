import logging

class logger():
    
    
    def __init__(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.WARNING)
        self.logger_ = logger
        
    def setLogLevel(self, args):
        level = getattr(logging, args.logging)()
        self.logger_.setLevel(level)
    
    def TEST_CONNECTION(self, function):
        def test(self):
            result = function(self)
            logging.info(f'the connection was set up successfully')
            logging.debug(f'connection data {args}')
            return result
        return test