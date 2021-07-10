import unittest
from unittest.mock import MagicMock
from pganonymizer.utils import get_connection
from pganonymizer.MainAnon import MainAnon
from pganonymizer.MainDeanon import MainDeanon
from pganonymizer.tests.utils.ConnectionMock import ConnectionMock
from pganonymizer.tests.utils.CursorMock import CursorMock
from pganonymizer.Args import Args
from queue import Queue

class TestCompleteProcess(unittest.TestCase):
    path_schema_anon = __file__.replace('integrationtest_anonymization.py', 'utils/integrationtest_anon.yml')
    path_schema_deanon = __file__.replace('integrationtest_anonymization.py', 'utils/integrationtest_deanon.yml')
    
    def get_current_data(self, anon):
        con = get_connection(anon.pg_args)
        con.autocommit = True
        cursor = con.cursor()
        cursor.execute("Select name, display_name, street from res_partner;")
        res_partner = cursor.fetchall()
        cursor.execute("Select name from res_company;")
        res_company = cursor.fetchall()
        return res_partner, res_company
    
    def test_anonymization(self):
        args = Args({'force_path_schema':self.path_schema_anon,
                     'analysis': False,
                     'type': 'anon',
                     'dbname': 'testdb'})
        anon = MainAnon(args)
        anon.jobs = Queue()
        partner, company = self.get_current_data(anon)
        anon.start_processing()
        partner_processed, company_processed = self.get_current_data(anon)
        for partner in partner_processed:
            self.assertTrue("res_partner_name_" in partner[0] if partner[0] != None else True, "partner not anonymized correctly")
            self.assertTrue("res_partner_display_name_" in partner[1] if partner[1] != None else True, "partner not anonymized correctly")
            self.assertTrue("res_partner_street_" in partner[2] if partner[2] != None else True, "partner not anonymized correctly")
        for company in company_processed:
            self.assertTrue("res_company_name_" in company[0] or company[0] == None, "company not anonymized correctly")
        
    def test_deanonymization(self):
        args = Args({'force_path_schema':self.path_schema_deanon,
             'analysis': False,
             'type': 'deanon',
             'dbname': 'testdb'})
        self.test_anonymization()
        anon = MainDeanon(args)
        anon.jobs = Queue()
        anon.start_processing()
        partner_processed, company_processed = self.get_current_data(anon)
        for partner in partner_processed:
            self.assertFalse("res_partner_name_" in partner[0] if partner[0] != None else False, "partner not anonymized correctly")
            self.assertFalse("res_partner_display_name_" in partner[1] if partner[1] != None else False, "partner not anonymized correctly")
            self.assertFalse("res_partner_street_" in partner[2] if partner[2] != None else False, "partner not anonymized correctly")
        for company in company_processed:
            self.assertFalse("res_company_name_" in company[0] if company[0] != None else False, "company not anonymized correctly")
        
        