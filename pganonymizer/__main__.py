#!/usr/bin/env python
from __future__ import absolute_import

from pganonymizer.constants import constants
from configparser import ConfigParser
from pganonymizer.cli import AnonymizationMain, DeAnonymizationMain
import time

import sys

class Args():
    def __init__(self, dic):
        self.verbose = dic.get("v")
        self.list_providers = dic.get("l")
        self.schema = dic.get("schema")
        self.dbname = dic.get("dbname")
        self.user = dic.get("user")
        self.password = dic.get("password")
        self.host = dic.get("host")
        self.port = dic.get("port")
        self.dry_run = dic.get("dry_run")
        self.dump_file = dic.get("dump")
        self.threading = dic.get('threading')

config = ConfigParser()
def main():
    time.sleep(5)
    testargs = {}
    config.read(constants.PATH_CONFIG_FILE)
    if False in set([x in config.sections() for x in constants.section]):
        raise Exception("Section not found!")
    [testargs.update({x : config.get("Required",x)}) for x in constants.testarg]
    [testargs.update({x : config.get("Optional",x)})for x in constants.testarg_optional if x in config['Optional']]
    args = Args(testargs)
    type = testargs.get('type')
    try:
        if type == 'anon':
            AnonymizationMain().startProcessing(args)
        elif type == 'deanon':
            DeAnonymizationMain().startProcessing(args)
        exit_status = 0
    except KeyboardInterrupt:
        exit_status = 1
    sys.exit(exit_status)

if __name__ == '__main__':
    main()
