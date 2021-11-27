'''Configurable interface to a datastore.'''

import logging

import psycopg2

class Database(object):
    '''Generic database connector for any interface that implements DB-API standards.'''
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def stage_object(self, target: object, target_table=False):
        if target_table is False:
            if not hasattr(target, 'target_table'):
                raise AttributeError(f"Target_table not defined in function call or target object -- set one of those.")
            target_table = target.target_table

        self.logger.info(f"Loading in {target} instance for interface with {target_table}.")

        self.logger.info(f"Trimming out columns not in {target}.model_columns. Starting with {len(target.data[0].keys())} columns...")
        target.formatted_data = []
        for entry in target.data:
            formatted_dict = {col: entry[col] for col in target.model_columns if col in entry}
            target.formatted_data.append(formatted_dict)
        self.logger.info(f"""Ended with {len(target.formatted_data[0].keys())} columns. Trimmed the following columns out:
                            {set(target.data[0].keys()) - set(target.formatted_data[0].keys())}""")

    def open_connection(self, creds: dict, connection_name=''):
        raise NotImplementedError("Implement the open_connection method on a per-connector basis.")

    def get_cursor(self, creds, connection_name=''):
        self.open_connection(creds, connection_name)
        self.cursor = self.cxn.cursor()
        self.logger.info("Cursor retrieved.")

    def close_connection(self):
        self.cursor.close()
        self.cxn.close()
        self.logger.info("Connection closed.")

class Postgres(Database):
    '''Connection to a postgres database.'''
    def __init__(self):
        super().__init__()

    def open_connection(self, creds: dict):
        self.logger.info(list(creds.keys()))
        if not set(['dbname', 'user', 'password', 'host', 'port']).issubset(set(creds.keys())):
            raise AttributeError("Required basic params for postgres connection not included in creds dict.")
        self.cxn = psycopg2.connect(**creds)
        self.logger.info(f"Set up connection to {creds['dbname']} Postgres db successfully.")

    def get_cursor(self, creds, cursor_type=False):
        '''
        Interface to connect to database and get a cursor. Will only connect
        if there is no existing connection.

        Can pass keywords to cursor type to get different kinds of cursors. Currently
        implemented: 'dictcursor' and default cursor type.
        '''
        if not hasattr(self, 'cxn'):
            self.open_connection(creds)

        if cursor_type == 'dictcursor':
            from psycopg2.extras import DictCursor
            self.cursor = self.cxn.cursor(cursor_factory=DictCursor)
        else:
            self.cursor = self.cxn.cursor()
        self.logger.info("Cursor retrieved.")

    def upsert_object(self, target_table, primary_key_list):
        pass
