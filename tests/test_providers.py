import unittest
from pganonymizer import providers


class TestChoiceProvider(unittest.TestCase):
    
    def test_alter_value(self):
        choiceprovider = providers.ChoiceProvider()
        values = [1,2,3,4]
        choosenvalue = choiceprovider.alter_value(**{'value':values})
        self.assertIn(choosenvalue,values)
    
class TestClearProvider(unittest.TestCase):
    def test_alter_value(self):
        clearprovider = providers.ClearProvider()
        choosenvalue = clearprovider.alter_value()
        self.assertEqual(choosenvalue,None)
    
class TestMigrationProvider(unittest.TestCase):
    def test_alter_value(self):
        migrationprovider = providers.MigrationProvider()
        choosenvalue = migrationprovider.alter_value(**{'id':15, 'field':'name', 'table':'res_partner'})
        self.assertEqual(choosenvalue,"res_partner_name_15")

class TestMaskProvider(unittest.TestCase):
    def test_alter_value(self):
        maskprovider = providers.MaskProvider()
        values = "test"
        choosenvalue = maskprovider.alter_value(**{'value':values})
        self.assertEqual(choosenvalue,"XXXX")
    
    def test_alter_value_correct_len(self):
       maskprovider = providers.MaskProvider()
       values = "thisisatestvalue"#len 17
       choosenvalue = maskprovider.alter_value(**{'value':values})
       self.assertEqual(len(choosenvalue),len(values))

class TestSetProvider(unittest.TestCase):
    def test_alter_value(self):
        setprovider = providers.SetProvider()
        values = "test"
        choosenvalue = setprovider.alter_value(**{'value':values})
        self.assertEqual(choosenvalue,"test")