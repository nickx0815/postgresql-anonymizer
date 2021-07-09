#!/usr/bin/env python
from __future__ import absolute_import

import os

from pganonymizer.constants import constants
from configparser import ConfigParser
from pganonymizer.MainAnon import MainAnon
from pganonymizer.MainDeanon import MainDeanon
from pganonymizer.Args import Args
from unittest import TestLoader
import unittest
import time

import sys, subprocess

config = ConfigParser()
def main():
    testargs = {}
    config.read(constants.PATH_CONFIG_FILE)
    if False in set([x in config.sections() for x in constants.section]):
        raise Exception("Section not found!")
    [testargs.update({x : config.get("Required",x)}) for x in constants.testarg]
    [testargs.update({x : config.get("Optional",x)})for x in constants.testarg_optional if x in config['Optional']]
    args = Args(testargs)
    type = args.type
    try:
        if type == 'anon':
            MainAnon(args).start_processing()
        elif type == 'deanon':
            MainDeanon(args).start_processing()
        else:
            raise Exception("the type has to be anon or deanon")
        exit_status = 0
    except KeyboardInterrupt:
        exit_status = 1
    sys.exit(exit_status)

if __name__ == '__main__':
    if '--unittest' in sys.argv:
        subprocess.call([sys.executable,  "-m", "unittest", "discover", path, "-p", "test_*.py"])
    if '--integrationtest' in sys.argv:
        path = os.path.abspath(os.path.join(os.path.dirname(__file__)).replace("pganonymizer", "tests/"))
        TestLoader_ = TestLoader()
        test_classes = TestLoader_.discover(path, pattern="integrationtest_*.py")
        unittest.TextTestRunner(verbosity=10).run(test_classes)
    if '--unittest' in sys.argv and '--integrationtest' in sys.argv:
        main()
