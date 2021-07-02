from unittest.mock import MagicMock
import unittest
from pganonymizer.utils import get_connection
from pganonymizer.AnonJob import AnonymizationMain
from pganonymizer.args import Args
from pganonymizer.constants import constants
from pganonymizer.logging import logger
from mock import patch



class TestMainJob(unittest.TestCase):
    path = __file__.replace('test_main_job.py', 'sample_schema.yml')
    
    def test_pg_args(self):
        pg_args = {'user': 'odoo', 'host': 'postgres', 'password':'odoo', 'port': 5432, 'dbname':'dbtest'}
        args = {'force_path_schema':self.path}
        args.update(pg_args)
        testmain = AnonymizationMain(Args(args)) 
        self.assertEqual(testmain.pg_args, pg_args)
    
    def test_get_schema(self):
        args = Args({'force_path_schema':self.path})
        testmain = AnonymizationMain(args)
        testmain.get_schema()
        self.assertEqual(testmain.schema['tables'], [{'auth_user': {'primary_key': 'id', 'fields': \
                                                                    [{'first_name': {'provider': {'name': 'fake.first_name'}}},\
                                                                     {'last_name': {'provider': {'name': 'set', 'value': 'Bar'}}},\
                                                                     {'email': {'provider': {'name': 'md5'}, 'append': '@localhost'}}], \
                                                    'excludes': [{'email': ['\\S[^@]*@example\\.com']}]}}])
        self.assertEqual(testmain.schema['truncate'], ['django_session'])
    
    def test_get_thread_number1(self):
        args = {'force_thread_number':2,'force_path_schema':self.path}
        with patch.object(AnonymizationMain, '_get_qsize', return_value=4) as mock_method:
            testmain = AnonymizationMain(Args(args))
            number  = testmain.get_thread_number()
            self.assertEqual(number, 2)
    
    def test_get_thread_number2(self):
        with patch.object(AnonymizationMain, '_get_qsize', return_value=4) as mock_method:
            testmain = AnonymizationMain(Args({'force_path_schema':self.path}))
            number  = testmain.get_thread_number()
            self.assertEqual(number, 4)
    
    def test_get_thread_number3(self):
        with patch.object(AnonymizationMain, '_get_qsize', return_value=10) as mock_method:
            testmain = AnonymizationMain(Args({'force_path_schema':self.path}))
            number  = testmain.get_thread_number()
            self.assertEqual(number, 8)
    
    
