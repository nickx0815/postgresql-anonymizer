import unittest, os, datetime
from unittest.mock import MagicMock
from pganonymizer.utils import get_connection, get_pg_args
from pganonymizer.MainAnon import MainAnon
from pganonymizer.MainDeanon import MainDeanon
from pganonymizer.constants import constants, date_pattern
from pganonymizer.tests.utils.ConnectionMock import ConnectionMock
from pganonymizer.tests.utils.CursorMock import CursorMock
from pganonymizer.Args import Args
from queue import Queue


class TestCompleteProcess(unittest.TestCase):
    path_schema_anon = __file__.replace('integrationtest_anonymization.py', 'utils/integrationtest_anon.yml')
    path_schema_deanon = __file__.replace('integrationtest_anonymization.py', 'utils/integrationtest_deanon.yml')
    
    args = {'analysis': False,
            'dbname': 'testdb',
            'migration': True
            }
    ERROR = "record not anonymized correctly"
    
    @classmethod
    def setUpClass(cls):
        super(TestCompleteProcess, cls).setUpClass()
        cls.args.update({'dbname':'testdb'})
        pgargs = get_pg_args(Args(cls.args))
        cls.original_data = cls.get_current_original_data(cls, pgargs)
    
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
            self.assertTrue(f"res_company_name_{company[0]}" == company[1] or company[1] == None, 
                            self.ERROR)
        
    def deanonymization(self):
        self.args.update({'force_path_schema':self.path_schema_deanon, 
                          'type': 'deanon'})
        args = Args(self.args)
        self.anonymization()
        anon = MainDeanon(args)
        anon.jobs = Queue()
        anon.start_processing()
        deanonymized_data = self.get_current_original_data(anon.pg_args)
        partner_original_data = self.original_data[0]
        company_original_data = self.original_data[1]
        for record in partner_original_data:
            self.assertTrue(record in deanonymized_data[0])
        self.assertTrue(len(partner_original_data) == len(deanonymized_data[0]))
        for record in company_original_data:
            self.assertTrue(record in deanonymized_data[1])
        self.assertTrue(len(company_original_data) == len(deanonymized_data[1]))
    
    def logging(self):
        log_lines = self.get_log_data()
        partner_original_data = self.original_data[0]
        company_original_data = self.original_data[1]
        for partner in partner_original_data:
            self.assertTrue(f"res_partner {partner[0]} name anonymized -> res_partner_name_{partner[0]}" in log_lines if partner[1] != None else True)
            self.assertTrue(f"res_partner {partner[0]} display_name anonymized -> res_partner_display_name_{partner[0]}" in log_lines if partner[2] != None else True)
            self.assertTrue(f"res_partner {partner[0]} street anonymized -> res_partner_street_{partner[0]}" in log_lines if partner[3] != None else True)
        for company in company_original_data:
            self.assertTrue(f"res_company {company[0]} name anonymized -> res_company_name_{company[0]}" in log_lines if company[1] != None else True)  
        for partner in partner_original_data:
            self.assertTrue(f"res_partner {partner[0]} name deanonymized -> {partner[1]}" in log_lines if partner[1] != None else True)
            self.assertTrue(f"res_partner {partner[0]} display_name deanonymized -> {partner[2]}" in log_lines if partner[2] != None else True)
            self.assertTrue(f"res_partner {partner[0]} street deanonymized -> {partner[3]}" in log_lines if partner[3] != None else True)
        for company in company_original_data:
            self.assertTrue(f"res_company {company[0]} name deanonymized -> {company[1]}" in log_lines if company[1] != None else True) 
            
    
    def test_process(self):
        self.anonymization()
        self.deanonymization()
        self.logging()

    def get_log_data(self):
        base_path = constants.PATH_LOG_FILE_BASE
        files = os.listdir(base_path)
        d = [file.replace(".log","") for file in files if file.endswith(".log")]
        sorted(d, key=lambda x: datetime.datetime.strptime(x, date_pattern))
        current_log = d[len(d)-1]
        with open(f"{base_path}{current_log}.log", "r") as log:
            log_lines = log.read()
        return log_lines

