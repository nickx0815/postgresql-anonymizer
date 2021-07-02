#!/usr/bin/env python
from __future__ import absolute_import

from pganonymizer.constants import constants
from configparser import ConfigParser
from pganonymizer.AnonJob import AnonymizationMain
from pganonymizer.DeanonJob import DeAnonymizationMain
from pganonymizer.args import Args
import time

import sys

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
