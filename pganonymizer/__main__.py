#!/usr/bin/env python
from __future__ import absolute_import

from pganonymizer.constants import constants
from configparser import ConfigParser
from pganonymizer.AnonJob import AnonJobClass
from pganonymizer.DeanonJob import DeanonJobClass
from pganonymizer.Args import Args
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
            AnonJobClass(args).start_processing()
        elif type == 'deanon':
            DeanonJobClass(args).start_processing()
        else:
            raise Exception("the type has to be anon or deanon")
        exit_status = 0
    except KeyboardInterrupt:
        exit_status = 1
    sys.exit(exit_status)

if __name__ == '__main__':
    main()
