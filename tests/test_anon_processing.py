import unittest
from pganonymizer.args import Args
from pganonymizer.MainJob import BaseMain
from pganonymizer.AnonProcessing import AnonProcessing

class TestAnonProcessing(unittest.TestCase):
    path = __file__.replace('test_anon_processing.py', 'utils/sample_schema_anon_processing_test.yml')
    
    def test_get_rel_method_tables(self):
        args = Args({'force_path_schema':self.path,
                     'FORCE_ANON_NUMBER_FIELD_PER_THREAD': 10})
        testmain = BaseMain(args)
        testprocess = AnonProcessing(testmain, "tables", 0, False, False, False)
        self.assertEqual(testprocess._get_rel_method(), "anonymize_tables")
    
    def test_get_rel_method_truncate(self):
        args = Args({'force_path_schema':self.path,
                     'FORCE_ANON_NUMBER_FIELD_PER_THREAD': 10})
        testmain = BaseMain(args)
        testprocess = AnonProcessing(testmain, "truncate", 0, False, False, False)
        self.assertEqual(testprocess._get_rel_method(), "truncate_tables")
    
    def test_get_column_dict(self):
        args = Args({'force_path_schema':self.path,
                     'FORCE_ANON_NUMBER_FIELD_PER_THREAD': 10})
        testmain = BaseMain(args)
        testprocess = AnonProcessing(testmain, "tables", 0, False, False, False)
        column_dict = testprocess.get_column_dict(testmain.schema['tables'][0]['table1']['fields'])
        for field in ['first_name', 'last_name', 'email']:
            column_dict[field]=None
    
    def test_get_column_values(self):
        args = Args({'force_path_schema':self.path,
                     'FORCE_ANON_NUMBER_FIELD_PER_THREAD': 10})
        testrow = {'first_name':'Testname','last_name':'Testnachname','email':'test@test.de'}
        row_info = {'table':'table1','id':1}
        testmain = BaseMain(args)
        testprocess = AnonProcessing(testmain, "tables", 0, False, False, False)
        column_dict = testprocess.get_column_values(testrow,
                                                    testmain.schema['tables'][0]['table1']['fields'],
                                                    row_info)
        self.assertTrue(testrow['first_name'] != column_dict['first_name'])
        self.assertTrue(testrow['last_name'] != column_dict['last_name'])
        self.assertTrue(testrow['email'] != column_dict['email'])
    
    def test_row_matches_excludes_true(self):
        args = Args({'force_path_schema':self.path,
                     'FORCE_ANON_NUMBER_FIELD_PER_THREAD': 10})
        testrow = {'first_name':'Testname','last_name':'Testnachname','email':'test@example.com'}
        testmain = BaseMain(args)
        testprocess = AnonProcessing(testmain, "tables", 0, False, False, False)
        result = testprocess.row_matches_excludes(testrow, excludes=testmain.schema['tables'][0]['table1']['excludes'])
        self.assertTrue(result)
    
    def test_row_matches_excludes_false(self):
        args = Args({'force_path_schema':self.path,
                     'FORCE_ANON_NUMBER_FIELD_PER_THREAD': 10})
        testrow = {'first_name':'Testname','last_name':'Testnachname','email':'test@test.com'}
        testmain = BaseMain(args)
        testprocess = AnonProcessing(testmain, "tables", 0, False, False, False)
        result = testprocess.row_matches_excludes(testrow, excludes=testmain.schema['tables'][0]['table1']['excludes'])
        self.assertFalse(result)
    
    def test_row_matches_excludes_id_true(self):
        args = Args({'force_path_schema':self.path,
                     'FORCE_ANON_NUMBER_FIELD_PER_THREAD': 10})
        testrow = {'id':10, 'first_name':'Testname','last_name':'Testnachname','email':'test@example.com'}
        testmain = BaseMain(args)
        testprocess = AnonProcessing(testmain, "tables", 0, False, False, False)
        result = testprocess.row_matches_excludes(testrow, excludes=testmain.schema['tables'][1]['table2']['excludes'])
        self.assertTrue(result)
    
    def test_row_matches_excludes__id_false(self):
        args = Args({'force_path_schema':self.path,
                     'FORCE_ANON_NUMBER_FIELD_PER_THREAD': 10})
        testrow = {'id':2, 'first_name':'Testname','last_name':'Testnachname','email':'test@test.com'}
        testmain = BaseMain(args)
        testprocess = AnonProcessing(testmain, "tables", 0, False, False, False)
        result = testprocess.row_matches_excludes(testrow, excludes=testmain.schema['tables'][1]['table2']['excludes'])
        self.assertFalse(result)
    
    def test_exclude_eval(self):
        return
    
    