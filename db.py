""" SQLite database to persist the values. """


import sqlite3
from time import time


class DB:
    def create_db(self):
        """ Create connection and table in SQLite DB. """
        file = f'databases/values_{int(time())}.db'
        open(file, 'w')  # TODO: 2 files are created
        self.conn = sqlite3.connect(file)
        self.cursor = self.conn.cursor()
        self.cursor.execute("""
            CREATE TABLE groups
            (id int, starting_timestamp int)
        """)
        self.cursor.execute("""
            CREATE TABLE topics
            (group_id int, topic text, value int)
        """)

    def connect_db(self, creation_time):
        file = f'databases/values_{creation_time}.db'
        self.conn = sqlite3.connect(file)
        self.cursor = self.conn.cursor()

    def get_cursor(self):
        return self.cursor

    def execute(self, command):
        self.cursor.execute(command)
        self.conn.commit()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()
