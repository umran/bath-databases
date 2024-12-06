from datetime import datetime

import sqlite3

# local imports
from .util import select_int_in_range, do_more, clear_stdout
from .airport import AirportTable
from .pilot import PilotTable
from .flight import FlightTable
from .flight_pilot import FlightPilotTable
from .derived_queries import flight_pilot_assignments, pilot_destination_frequencies, pilot_schedule, unassigned_pilots

class Console:
    airport_table: AirportTable
    pilot_table: PilotTable
    flight_table: FlightTable
    flight_pilot_table: FlightPilotTable

    def __init__(self):
        # Register the datetime adapter and converter to suppress the deprecation warning
        sqlite3.register_adapter(datetime, adapt_datetime)
        sqlite3.register_converter("DATETIME", convert_datetime)

        self.airport_table = AirportTable()
        self.pilot_table = PilotTable()
        self.flight_table = FlightTable()
        self.flight_pilot_table = FlightPilotTable()

    def run(self):
        with sqlite3.connect("airline.db") as conn:
            # use sqlite3.Row as row_factory to be able to access columns by name
            conn.row_factory = sqlite3.Row
            # enable foreign key support explicitly so that we can enforce foreign key constraints
            conn.execute("PRAGMA foreign_keys = ON")
            
            self.migrate(conn)
            
            while True:
                try:
                    self.select_option(conn)
                except sqlite3.IntegrityError:
                    print("An invalid update was prevented from violating a primary key or unique key constraint")
                except Exception as e:
                    print("An unrecoverable error occurred. this is most likely due to a bug in the code. My sincere apologies :(")
                    raise e
                
                if do_more() is False:
                    return

    def select_option(self, conn: sqlite3.Connection):
        clear_stdout()
        
        options = [
            ("Add New Airport", self.airport_table.create_record),
            ("Update Existing Airport", self.airport_table.update_record),
            ("Add New Pilot", self.pilot_table.create_record),
            ("Update Existing Pilot", self.pilot_table.update_record),
            ("Add New Flight", self.flight_table.create_record),
            ("Update Existing Flight", self.flight_table.update_record),
            ("Assign Pilot to Flight", self.flight_pilot_table.assign_pilot_to_flight),
            ("Unassign Pilot from Flight", self.flight_pilot_table.unassign_pilot_from_flight),
            ("List Assigned Pilots for Flight", flight_pilot_assignments),
            ("Show Pilot Schedule", pilot_schedule),
            ("List Frequency of Pilot Destinations", pilot_destination_frequencies),
        ]

        print("Please select an option from the list below")
        
        for idx, (name, _) in enumerate(options):
            print(f"    ({idx + 1}). {name}")
            
        # this should result in an idx within the correct bounds
        selected_idx = select_int_in_range("Please enter an option number: ", 1, len(options)) - 1
        
        _, endpoint = options[selected_idx]

        endpoint(conn)

    def migrate(self, conn: sqlite3.Connection):
        self.airport_table.create_table(conn)
        self.pilot_table.create_table(conn)
        self.flight_table.create_table(conn)
        self.flight_pilot_table.create_table(conn)
            
            

# Define adapter for datetime -> string
def adapt_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# Define converter for string -> datetime
def convert_datetime(s: bytes) -> datetime:
    return datetime.strptime(s.decode("utf-8"), "%Y-%m-%d %H:%M:%S")

def test():
    Console().run()

test()