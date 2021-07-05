"""Commandline implementation"""

from __future__ import absolute_import, print_function

import logging
import copy
import datetime
import time

from pganonymizer.constants import constants 
from pganonymizer.AnonProcessing import AnonProcessing
from pganonymizer.utils import build_sql_select, _get_ids_sql_format, create_basic_tables
from pganonymizer.MainJob import BaseJobClass
from pganonymizer.AnonProcessing import AnonProcessing

class AnonJobClass(BaseJobClass):
    
    def __init__(self, args):
        super(AnonJobClass, self).__init__(args)
        self.set_anon_fetch_records(args)
        self.set_anon_number_field_per_thread(args)
        
    def set_anon_fetch_records(self, args):
        self.ANON_FETCH_RECORDS = args.FORCE_ANON_FETCH_RECORDS if args.FORCE_ANON_FETCH_RECORDS \
            else constants.ANON_FETCH_RECORDS
    
    def set_anon_number_field_per_thread(self, args):
        self.ANON_NUMBER_FIELD_PER_THREAD=  args.FORCE_ANON_NUMBER_FIELD_PER_THREAD if args.FORCE_ANON_NUMBER_FIELD_PER_THREAD \
            else constants.ANON_NUMBER_FIELD_PER_THREAD
            
    def get_anon_fetch_records(self):
        return self.ANON_FETCH_RECORDS
    
    def get_anon_number_field_per_thread(self):
        return self.ANON_FETCH_RECORDS
    
    def get_args(self):
        parser =  BaseJobClass.get_args(self, parseArgs=False)
        parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity')
        parser.add_argument('-l', '--list-providers', action='store_true', help='Show a list of all available providers',
                            default=False)
        parser.add_argument('--dump-file', help='Create a database dump file with the given name')
        args = parser.parse_args()
        return args
    
    def create_basic_tables(self, connection, tables=[constants.TABLE_MIGRATED_DATA], suffix=False):
        create_basic_tables(connection, tables=[constants.TABLE_MIGRATED_DATA], suffix=suffix)
    
    def update_queue(self):
        #todo konfigurierbar
        #search wird nicht übernommen
        connection = self.get_connection()
        for type_, type_attributes in self.schema.items():
            for table in type_attributes:
                if type(table) == str:
                    self.jobs.put(AnonProcessing(self, type_, 1, [table], table, self.pg_args))
                else:
                    for table_key, table_attributes in table.items():
                        self.create_basic_tables(self.get_connection(), tables=[constants.TABLE_MIGRATED_DATA], suffix=table_key)
                        test = self.update_anon_search(table_key, table_attributes)
                        cursor = self.build_sql_select(connection, table_key, test.get('search', False), select="id")
                        while True:
                            list = []
                            records = cursor.fetchmany(size=self.ANON_NUMBER_FIELD_PER_THREAD)
                            totalrecords = len(records)
                            if not records:
                                break
                            for row in records:
                                list.append(row.get('id'))
                            table_attributes_job = self.add_job_records_ids(table_attributes, list)
                            self.jobs.put(AnonProcessing(self, type_, totalrecords, table_attributes_job, table_key, self.pg_args))
        connection.close()
    
    def build_sql_select(self, connection, table_key, search, select="id"):
        return build_sql_select(connection, table_key, search, select=select)
    
    def add_job_records_ids(self, table_attributes, ids):
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
