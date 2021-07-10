import unittest
from unittest.mock import MagicMock
from pganonymizer.utils import get_connection
from pganonymizer.MainAnon import MainAnon
from pganonymizer.tests.utils.ConnectionMock import ConnectionMock
from pganonymizer.tests.utils.CursorMock import CursorMock
from pganonymizer.Args import Args


class TestAnonJobClass(unittest.TestCase):
    path = __file__.replace('test_anon_job.py', 'utils/sample_schema.yml')

    
    def test_update_queue(self):
        args = Args({'force_path_schema':self.path,
                     'FORCE_ANON_NUMBER_FIELD_PER_THREAD': 10})
        testmain = MainAnon(args)
        testmain.get_connection = MagicMock(return_value=ConnectionMock())
        testmain.create_basic_table = MagicMock(return_value=True)
        testmain.build_sql_select = MagicMock(return_value=CursorMock())
        testmain.update_queue()
        #todo assert
    
    def test_addJobRecordIds(self):
        args = Args({'force_path_schema':self.path})
        testmain = MainAnon(args)
        tableattr = testmain.schema.get('anonymization')[0].get('auth_user')
        tableattr = testmain.add_job_records_ids(tableattr, [1,2,3,4,5,6])
        self.assertIn('id in (1, 2, 3, 4, 5, 6)', tableattr['search'])
    
    def test_update_anon_search(self):
        args = Args({'force_path_schema':self.path})
        testmain = MainAnon(args)
        tableattr = testmain.schema.get('anonymization')[0].get('auth_user')
        tableattr = testmain.update_anon_search('auth_user', tableattr)
        self.assertIn("first_name not like 'auth_user_first_name_%' AND first_name IS NOT NULL", tableattr['search'][0])
        self.assertIn("last_name not like 'auth_user_last_name_%' AND last_name IS NOT NULL", tableattr['search'][0])
        self.assertIn("email not like 'auth_user_email_%' AND email IS NOT NULL", tableattr['search'][0])