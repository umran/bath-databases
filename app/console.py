import sqlite3

class Console:
    conn: sqlite3.Connection

    def __init__(self):
        conn = sqlite3.connect("airline.db")

        # use sqlite3.Row as row_factory to be able to access columns by name
        conn.row_factory = sqlite3.Row

        # enable foreign key support explicitly so that we can enforce foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON")
        
        self.conn = conn