'''Configurable interface to a datastore.'''

#### LOGGING PLEASE


class Database(object):
    '''Generic database connector that implements DB-API standards.'''
    def __init__(self):
        pass

    def stage_object(self):
        pass

    def open_connection(self):
        pass

    def get_cursor(self):
        self.open_connection()
        self.cursor = self.cxn.cursor()

    def close_connection(self):
        pass

class Postgres(Database):
    '''Connection to a postgres database.'''
    def __init__(self):
        super().__init__()

    def stage_object(self, object, primary_key_column):
        # override?
        pass

    def upsert_object(self, target_table):
        pass
