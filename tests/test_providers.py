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
        values = "test"
        choosenvalue = clearprovider.alter_value(**{'value':values})
        self.assertEqual(choosenvalue,False)
    
class TestMigrationProvider(unittest.TestCase):
    def test_alter_value(self):
        migrationprovider = providers.MigrationProvider()
        values = "test"
        choosenvalue = migrationprovider.alter_value(**{'value':values, 'row':{'id':15, 'field':'name', 'table':'res_partner'}})
        self.assertEqual(choosenvalue,"res_partner_name_15")

class TestMaskProvider(unittest.TestCase):
    def test_alter_value(self):
        MaskProvider = providers.MaskProvider()
        values = "test"
        choosenvalue = MaskProvider.alter_value(**{'value':values})
        self.assertEqual(choosenvalue,"XXXX")

class TestSetProvider(unittest.TestCase):
    def test_alter_value(self):
        clearprovider = providers.ClearProvider()
        values = "test"
        choosenvalue = clearprovider.alter_value(**{'value':values})
        self.assertEqual(choosenvalue,"test")