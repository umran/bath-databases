from datetime import datetime

import sqlite3

# local imports
from .airport import AirportTable
from .pilot import PilotTable
from .flight import FlightTable
from .flight_pilot import FlightPilotTable

class Console:
    conn: sqlite3.Connection
    airport_table: AirportTable
    pilot_table: PilotTable
    flight_table: FlightTable
    flight_pilot_table: FlightPilotTable

    def __init__(self):
        # Register the datetime adapter and converter to suppress the deprecation warning
        sqlite3.register_adapter(datetime, adapt_datetime)
        sqlite3.register_converter("DATETIME", convert_datetime)

        conn = sqlite3.connect("airline.db")

        # use sqlite3.Row as row_factory to be able to access columns by name
        conn.row_factory = sqlite3.Row

        # enable foreign key support explicitly so that we can enforce foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON")

        self.conn = conn
        self.airport_table = AirportTable()
        self.pilot_table = PilotTable()
        self.flight_table = FlightTable()
        self.flight_pilot_table = FlightPilotTable()

    def run():
        while True:
            pass

    def menu(self):
        options = [
            ("Create Airport", self.airport_table.create_record),
            ("Update Airport", self.airport_table.update_record),
            ("Create Pilot", self.pilot_table.create_record),
            ("Update Pilot", self.pilot_table.update_record),
            ("Create Flight", self.flight_table.create_record),
            ("Update Flight", self.flight_table.update_record),
            ("Assign Pilot to Flight", 0)
        ]
    
    def assign_pilot_to_flight(self):
        maybe_pilot_id = self.pilot_table.table_def.select_record_from_db(self.conn.cursor())

        if maybe_pilot_id is None:
            return
        
        maybe_flight_id = self.pilot_table.table_def.select_record_from_db(self.conn.cursor())

        if maybe_flight_id is None:
            return
        
        self.flight_pilot_table.create_record(maybe_flight_id["id"], maybe_pilot_id["id"])
        

# Define adapter for datetime -> string
def adapt_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# Define converter for string -> datetime
def convert_datetime(s: bytes) -> datetime:
    return datetime.strptime(s.decode("utf-8"), "%Y-%m-%d %H:%M:%S")