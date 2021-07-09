import unittest
from unittest.mock import MagicMock
from pganonymizer.utils import get_connection
from pganonymizer.MainAnon import MainAnon
from pganonymizer.tests.utils.ConnectionMock import ConnectionMock
from pganonymizer.tests.utils.CursorMock import CursorMock
from pganonymizer.Args import Args
from queue import Queue

class TestCompleteProcess(unittest.TestCase):
    path = __file__.replace('integrationtest_anonymization.py', 'utils/integrationtest.yml')
    
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
        args = Args({'force_path_schema':self.path,
                     'threading':False,
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
        