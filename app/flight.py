import sqlite3
from datetime import datetime

# local imports
from .table import TableDef, ColumnDef, DataType, Value
from .airport import AirportTable
from .util import binary_decision

class FlightTable():
    table_def: TableDef

    def __init__(self):
        self.table_def = TableDef("flights", [
            ColumnDef("id", DataType.Int),
            ColumnDef("flight_number", DataType.Text),
            ColumnDef("date", DataType.Date),
            ColumnDef(
                "status",
                DataType.Text,
                allowed_values=[
                    Value.new_text("scheduled"),
                    Value.new_text("delayed"),
                    Value.new_text("boarding"),
                    Value.new_text("departed"),
                    Value.new_text("arrived")
                ]
            ),
            ColumnDef("departure_time", DataType.DateTime),
            ColumnDef("arrival_time", DataType.DateTime),
            ColumnDef("origin_id", DataType.Int),
            ColumnDef("destination_id", DataType.Int)
        ])
    
    def create_record(self, conn: sqlite3.Connection):
        # first get the values of all non-auto and non-foreign key fields
        values = self.table_def.get_column_values(lambda col: col.name not in ["id", "origin_id", "destination_id"])
        
        airport_table = AirportTable()

        print("Please take a moment to select the flight origin")
        maybe_origin = airport_table.table_def.select_record_from_db(conn.cursor())
        if maybe_origin is None:
            return
        
        print("Please take a moment to select the flight destination")
        maybe_destination = airport_table.table_def.select_record_from_db(conn.cursor())
        if maybe_destination is None:
            return

        values.append(maybe_origin["id"])
        values.append(maybe_destination["id"])

        statement = f"""
            INSERT INTO flights 
            (flight_number, date, status, departure_time, arrival_time, origin_id, destination_id)
            VALUES
            (?, ?, ?, ?, ?, ?, ?) 
        """

        conn.execute(statement, [value.inner for value in values])
        conn.commit()

    def update_record(self, conn: sqlite3.Connection):
        # first, select a record to update
        print("Select flight to update:")
        record = self.table_def.select_record_from_db(conn.cursor())

        updateable_columns = [col for col in self.table_def.columns if col.name not in ["id"]]

        # get a handle to the airport table representation so that we can display airports in
        # case the user wants to update the origin_id and destination_id
        airport_table = AirportTable()

        for column in updateable_columns:
            if binary_decision(f"would you like to update {column.name}? "):
                if column.name in ["origin_id", "destination_id"]:
                    val = airport_table.table_def.select_record_from_db(conn.cursor())
                    if val is None:
                        continue
                    else:
                        record[column.name] = val
                else:
                    val = self.table_def.get_value(f"Please enter the new value for {column.name}")
                    record[column.name] = val
            print("the flight will be updated to reflect the following values:")
            for key in record.keys():
                print(f"    {key}:      {record[key].to_str()}")
            if binary_decision("would you like to proceed with these changes?"):
                update_set = [f"{column.name} = ?" for column in updateable_columns]
                update_values = [record[column.name].inner for column in updateable_columns]

                


    def select_record(self, cursor: sqlite3.Cursor):
        record = self.table_def.select_record_from_db(cursor)
        print(record)

    def create_table(self, conn: sqlite3.Connection):
        statement = """
            CREATE TABLE IF NOT EXISTS flights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                flight_number TEXT NOT NULL,
                date DATE NOT NULL,
                status TEXT NOT NULL,
                departure_time DATETIME NOT NULL,
                arrival_time DATETIME NOT NULL,
                origin_id INTEGER NOT NULL,
                destination_id INTEGER NOT NULL,
                FOREIGN KEY (origin_id) REFERENCES airports(id),
                FOREIGN KEY (destination_id) REFERENCES airports(id),
                UNIQUE (flight_number, date)
            )
        """

        conn.execute(statement)
        conn.commit()

def test():
    # Define adapter for datetime -> string
    def adapt_datetime(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    # Define converter for string -> datetime
    def convert_datetime(s: bytes) -> datetime:
        return datetime.strptime(s.decode("utf-8"), "%Y-%m-%d %H:%M:%S")

    # Register the adapter and converter
    sqlite3.register_adapter(datetime, adapt_datetime)
    sqlite3.register_converter("DATETIME", convert_datetime)

    conn = sqlite3.connect("airline.db")

    # use sqlite3.Row as row_factory to be able to access columns by name
    conn.row_factory = sqlite3.Row

    # enable foreign key support explicitly so that we can enforce foreign key constraints
    conn.execute("PRAGMA foreign_keys = ON")

    flight_table = FlightTable()
    flight_table.create_table(conn)

    flight_table.create_record(conn)

    # flight_table.select_record(conn.cursor())

test()