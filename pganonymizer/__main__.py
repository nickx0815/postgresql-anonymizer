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


def __main():
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

def run_test(p):
    path = os.path.abspath(os.path.join(os.path.dirname(__file__)).replace("pganonymizer", "tests/"))
    testloader_ = TestLoader()
    test_classes = testloader_.discover(path, pattern=p)
    unittest.TextTestRunner(verbosity=10).run(test_classes)

def main():
    print(sys.argv)
    if '--unittest' in sys.argv:
        pattern =  "test_*.py"
        run_test(pattern)
    if '--integrationtest' in sys.argv:
        pattern = "integrationtest_*.py"
        run_test(pattern)
    if not '--unittest' in sys.argv and not '--integrationtest' in sys.argv:
        __main()

