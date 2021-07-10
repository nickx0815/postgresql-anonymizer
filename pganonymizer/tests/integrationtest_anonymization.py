import unittest
from unittest.mock import MagicMock
from pganonymizer.utils import get_connection, get_pg_args
from pganonymizer.MainAnon import MainAnon
from pganonymizer.MainDeanon import MainDeanon
from pganonymizer.tests.utils.ConnectionMock import ConnectionMock
from pganonymizer.tests.utils.CursorMock import CursorMock
from pganonymizer.Args import Args
from queue import Queue


class TestCompleteProcess(unittest.TestCase):
    path_schema_anon = __file__.replace('integrationtest_anonymization.py', 'utils/integrationtest_anon.yml')
    path_schema_deanon = __file__.replace('integrationtest_anonymization.py', 'utils/integrationtest_deanon.yml')
    
    args = {'analysis': False,
            'dbname': 'testdb',
            'migration': True}
    original_data = False
    ERROR = "record not anonymized correctly"
    
    @classmethod
    def setUpClass(cls):
        super(TestCompleteProcess, cls).setUpClass()
        cls.original_data = cls.get_current_original_data(cls, get_pg_args(Args({'dbname':'testdb'})))
    
    def get_current_original_data(self, args):
        con = get_connection(args)
        cursor = con.cursor()
        cursor.execute("Select id, name, display_name, street from res_partner;")
        res_partner = cursor.fetchall()
        cursor.execute("Select id, name from res_company;")
        res_company = cursor.fetchall()
        return res_partner, res_company
    
    def anonymization(self):
        self.args.update({'force_path_schema':self.path_schema_anon, 
                          'type': 'anon'})
        args = Args(self.args)
        anon = MainAnon(args)
        anon.jobs = Queue()
        anon.start_processing()
        partner_processed, company_processed = self.get_current_original_data(anon.pg_args)
        for partner in partner_processed:
            self.assertTrue(f"res_partner_name_{partner[0]}" == partner[1] if partner[1] != None else True, 
                            )
            self.assertTrue(f"res_partner_display_name_{partner[0]}" == partner[2] if partner[2] != None else True, 
                            self.ERROR)
            self.assertTrue(f"res_partner_street_{partner[0]}" == partner[3] if partner[3] != None else True, 
                            self.ERROR)
        for company in company_processed:
            self.assertTrue(f"res_company_name_{partner[0]}" == company[1] or company[1] == None, 
                            self.ERROR)
        
    def test_deanonymization(self):
        self.args.update({'force_path_schema':self.path_schema_deanon, 
                          'type': 'deanon'})
        args = Args(self.args)
        self.anonymization()
        anon = MainDeanon(args)
        anon.jobs = Queue()
        anon.start_processing()
        deanonymized_data = self.get_current_original_data(anon.pg_args)[0]
        original_data = self.original_data[0]
        for record in original_data:
            self.assertTrue(record in deanonymized_data)
        
        
