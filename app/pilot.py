from typing import List
import sqlite3

# local imports
from .table import TableDef, ColumnDef, DataType, Value

class PilotTable():
    table_def: TableDef

    def __init__(self):
        self.table_def = TableDef("pilots", [
            ColumnDef("id", DataType.Int),
            ColumnDef("name", DataType.Text),
            ColumnDef("age", DataType.Int),
            ColumnDef(
                "home_airport_id",
                DataType.Int,
                # allowed_values=[
                #     Value.new_text("scheduled"),
                #     Value.new_text("delayed"),
                #     Value.new_text("boarding"),
                #     Value.new_text("departed"),
                #     Value.new_text("arrived")
                # ]
            ),
            # ColumnDef("departure_time", DataType.DateTime),
            # ColumnDef("arrival_time", DataType.DateTime),
            # ColumnDef("origin_id", DataType.Int),
            # ColumnDef("destination_id", DataType.Int)
        ])
    
    def create_record(self, conn: sqlite3.Connection):
        # get the values of all non-auto fields from user input
        values = self.table_def.get_column_values(lambda col: col.name not in ["id"])
        
        statement = f"""
            INSERT INTO airports
                (icao_code, name, city)
            VALUES
                (?, ?, ?) 
        """

        conn.execute(statement, [value.inner for value in values])
        conn.commit()

    def find_records(self, cursor: sqlite3.Cursor):
        records = self.table_def.find_records_from_db(cursor)
        print(records)

    def select_record(self, cursor: sqlite3.Cursor):
        record = self.table_def.select_record_from_db(cursor)
        print(record)
        

    def create_table(self, conn: sqlite3.Connection):
        statement = """
            CREATE TABLE IF NOT EXISTS airports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                icao_code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                city TEXT NOT NULL
            )
        """

        conn.execute(statement)
        conn.commit()

def test():
    conn = sqlite3.connect("airline.db")

    # use sqlite3.Row as row_factory to be able to access columns by name
    conn.row_factory = sqlite3.Row

    # enable foreign key support explicitly so that we can enforce foreign key constraints
    conn.execute("PRAGMA foreign_keys = ON")

    airport_table = PilotTable()
    airport_table.create_table(conn)

    airport_table.create_record(conn)

    # airport_table.select_record(conn.cursor())

# test()
