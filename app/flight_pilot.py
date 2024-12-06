import sqlite3

# local imports
from .table import TableDef, ColumnDef, DataType

class FlightPilotTable():
    table_def: TableDef

    def __init__(self):
        self.table_def = TableDef("flight_pilot", [
            ColumnDef("flight_id", DataType.Int),
            ColumnDef("pilot_id", DataType.Int)
        ])

    def create_record(self, conn: sqlite3.Connection, flight_id: int, pilot_id: int):
        statement = f"""
            INSERT INTO flight_pilot
                (flight_id, pilot_id)
            VALUES
                (?, ?) 
        """

        conn.execute(statement, [flight_id, pilot_id])
        conn.commit()

    def create_table(self, conn: sqlite3.Connection):
        statement = """
            CREATE TABLE IF NOT EXISTS flight_pilot (
                flight_id INTEGER PRIMARY KEY,
                pilot_id INTEGER PRIMARY KEY,
                FOREIGN KEY (flight_id) REFERENCES flight(id),
                FOREIGN KEY (pilot_id) REFERENCES pilot(id)
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

    flight_pilot_table = FlightPilotTable()
    flight_pilot_table.create_table(conn)

    flight_pilot_table.update_record(conn)

    # flight_pilot_table.select_record(conn.cursor())

# test()
