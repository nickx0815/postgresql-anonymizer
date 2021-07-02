from unittest.mock import MagicMock
from pganonymizer.utils import get_connection
from pganonymizer.AnonJob import AnonymizationMain
from pganonymizer.args import Args

class TestGetSchema():
    args = Args({'force_schema_path':'../sample_schema.yml'})
    testmain = AnonymizationMain(args)
    testmain.update_queue = MagicMock(return_value=True)
    print(testmain.update_queue())
    
    
TestGetSchema()