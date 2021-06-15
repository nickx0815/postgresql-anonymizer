#!/usr/bin/env python
from __future__ import absolute_import
from configparser import ConfigParser

import sys

config = ConfigParser()
def main():
    config.read('/home/migration/migrationConfig.conf')
    print([x for x in config.sections()])
    #from pganonymizer.cli import main_anonymize
    try:
        #main_anonymize()
        exit_status = 0
    except KeyboardInterrupt:
        exit_status = 1
    sys.exit(exit_status)


if __name__ == '__main__':
    main()
