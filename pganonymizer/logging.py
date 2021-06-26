import logging

class logger():
    
    
    def __init__(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.Warning)
        self.logger_ = logger
        
    def setLogLevel(self, args):
        level = getattr(logging, args.logging)()
        self.logger_.setLevel(level)
    
    def TEST_CONNECTION(self, function):
        def test():
            print("jo")
        return test