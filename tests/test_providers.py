import unittest
from pganonymizer.providers import ChoiceProvider


class TestChoiceProvider(unittest.TestCase):
    
    def test_alter_value(self):
        choiceProvider = ChoiceProvider()
        values = [1,2,3,4]
        choosenvalue = choiceProvider.alter_value(**{'value':values})
        self.assertIn(choosenvalue,values)
    
class TestClearProvider(unittest.TestCase):
    def test_alter_value(self):
        return
    
class TestMigrationProvider(unittest.TestCase):
    def test_alter_value(self):
        return

class TestFakeProvider(unittest.TestCase):
    def test_alter_value(self):
        return

class TestMaskProvider(unittest.TestCase):
    def test_alter_value(self):
        return

class TestMD5Provider(unittest.TestCase):
    def test_alter_value(self):
        return

class TestSetProvider(unittest.TestCase):
    def test_alter_value(self):
        return