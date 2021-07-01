"""Commandline implementation"""

from __future__ import absolute_import, print_function

import logging
import sys
import copy
import datetime
import time

from pganonymizer.constants import constants 
from pganonymizer.AnonProcessing import AnonProcessing
from pganonymizer.utils import get_connection, build_sql_select, _get_ids_sql_format, create_basic_tables
from pganonymizer.MainJob import BaseMain
from pganonymizer.AnonProcessing import AnonProcessing

class AnonymizationMain(BaseMain):
    
    THREAD = "NUMBER_MAX_THREADS_ANON"
    
    def get_args(self):
        parser =  BaseMain.get_args(self, parseArgs=False)
        parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity')
        parser.add_argument('-l', '--list-providers', action='store_true', help='Show a list of all available providers',
                            default=False)
        parser.add_argument('--dump-file', help='Create a database dump file with the given name')
        args = parser.parse_args()
        return args
    
    def __init__(self, args):
        if args.list_providers:
            self.list_provider_classes()
            sys.exit(0)
        BaseMain.__init__(self, args)
    
    def update_queue(self):
        #todo konfigurierbar
        #search wird nicht übernommen
        pg_args = self.pg_args
        connection = get_connection(pg_args)
        schema = self.schema
        for type_, type_attributes in schema.items():
            for table in type_attributes:
                if type(table) == str:
                    self.jobs.put(AnonProcessing(self, type_, 1, [table], table, pg_args,self.logger))
                else:
                    for table_key, table_attributes in table.items():
                        create_basic_tables(get_connection(self.pg_args), tables=[constants.TABLE_MIGRATED_DATA], suffix=table_key)
                        test = self.update_anon_search(table_key, table_attributes)
                        cursor = build_sql_select(connection, table_key, test.get('search', False), select="id")
                        while True:
                            list = []
                            records = cursor.fetchmany(size=constants.ANON_NUMBER_FIELD_PER_THREAD)
                            totalrecords = len(records)
                            if not records:
                                break
                            for row in records:
                                list.append(row.get('id'))
                            table_attributes_job = self.addJobRecordIds(table_attributes, list)
                            self.jobs.put(AnonProcessing(self, type_, totalrecords, table_attributes_job, table_key, pg_args, self.logger))
        connection.close()
    
    def addJobRecordIds(self, table_attributes, ids):
        cur = copy.deepcopy(table_attributes)    
        search_list = cur.get('search', [])
        search_list.append("id in "+_get_ids_sql_format(ids))
        cur['search'] = search_list
        return cur
    
    def update_anon_search(self, table, table_attributes):
        table_attributes = copy.deepcopy(table_attributes)   
        fielddic = table_attributes.get('fields')
        search = table_attributes.get('search', [])
        if fielddic:
            searchlist = []
            fieldlist = [list(x.keys())[0] for x in fielddic]
            for field in fieldlist:
                searchlist.append(f"{field} not like '{table}_{field}_' AND {field} IS NOT NULL ")
            search.append("("+" OR ".join(searchlist)+")")
        table_attributes['search'] = search
        return table_attributes
