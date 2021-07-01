#!/usr/bin/env python
from __future__ import absolute_import

from pganonymizer.constants import constants
from configparser import ConfigParser
from pganonymizer.AnonJob import AnonymizationMain
from pganonymizer.DeanonJob import DeAnonymizationMain
import time

import sys

class Args():
    def __init__(self, dic):
        self.verbose = dic.get("v")
        self.list_providers = dic.get("l")
        self.schema = dic.get("schema")
        self.dbname = dic.get("dbname")
        self.user = dic.get("user", "odoo")
        self.password = dic.get("password", "odoo")
        self.host = dic.get("host", "postgres")
        self.port = dic.get("port", 5432)
        self.dry_run = dic.get("dry_run")
        self.dump_file = dic.get("dump")
        self.threading = dic.get('threading', True)
        self.force_path_schema = dic.get('force_path_schema')
        self.logging = dic.get('logging', 'INFO')
        self.migration = dic.get('migration')
        self.type = dic.get('type')

config = ConfigParser()
def main():
    testargs = {}
    config.read(constants.PATH_CONFIG_FILE)
    if False in set([x in config.sections() for x in constants.section]):
        raise Exception("Section not found!")
    [testargs.update({x : config.get("Required",x)}) for x in constants.testarg]
    [testargs.update({x : config.get("Optional",x)})for x in constants.testarg_optional if x in config['Optional']]
    args = Args(testargs)
    type = args.get('type')
    try:
        if type == 'anon':
            AnonymizationMain(args).startprocessing()
        elif type == 'deanon':
            DeAnonymizationMain(args).startprocessing()
        else:
            raise Exception("the type has to be anon or deanon")
        exit_status = 0
    except KeyboardInterrupt:
        exit_status = 1
    sys.exit(exit_status)

if __name__ == '__main__':
    main()
