from datetime import datetime

import sqlite3

# local imports
from .table import TableDef, ColumnDef, DataType
from .flight import FlightTable
from .pilot import PilotTable

# this is a query that lists all pilots who have not been assigned to a flight
# by left joining the pilot table with the flight_pilot table, which ensures that
# the pilot records will be produced even if the joining table on the right side is null
# this query does an additional left join with the airport table on pilot.home_airport_id = airport.id
# in order to list the airport ICAO code of the pilot's home_airport
def unassigned_pilots(conn: sqlite3.Connection):
    statement = """
        SELECT 
            p.id AS pilot_id, 
            p.name AS name,
            a.icao_code AS home_airport
        FROM pilot p
        LEFT JOIN flight_pilot fp ON p.id = fp.pilot_id
        LEFT JOIN airport a ON p.home_airport_id = a.id
        WHERE fp.flight_id IS NULL
    """

    table = TableDef("unassigned_pilots", [
        ColumnDef("pilot_id", DataType.Int),
        ColumnDef("name", DataType.Text),
        ColumnDef("home_airport", DataType.Text),
    ])

    results = table.find_records(conn.cursor(), statement)
    table.display_records(results)

# this is a query that produces information about which pilots have been assigned to a particular flight, including
# if nobody has been assigned to the flight. This is because we are using a left join on the flights table, using the flight_pilot junction table to
# join on pilots.
def flight_pilot_assignments(conn: sqlite3.Connection):
    flight_table = FlightTable()

    print("Please select a flight: ")
    maybe_flight = flight_table.table_def.select_record(conn.cursor())

    if maybe_flight is None:
        return

    statement = """
        SELECT 
            p.id AS pilot_id, 
            p.name AS pilot_name,
            f.flight_number, 
            f.date,
            f.status,
            origin.icao_code AS origin,
            destination.icao_code AS destination
        FROM flight f
        LEFT JOIN flight_pilot fp ON f.id = fp.flight_id
        LEFT JOIN pilot p ON fp.pilot_id = p.id
        LEFT JOIN airport origin ON f.origin_id = origin.id
        LEFT JOIN airport destination ON f.destination_id = destination.id
        WHERE f.id = ?;
    """

    table = TableDef("flight_pilot_assignments", [
        ColumnDef("pilot_id", DataType.Int, nullable=True),
        ColumnDef("pilot_name", DataType.Text, nullable=True),
        ColumnDef("flight_number", DataType.Text),
        ColumnDef("date", DataType.Date),
        ColumnDef("status", DataType.Text),
        ColumnDef("origin", DataType.Text),
        ColumnDef("destination", DataType.Text)
    ])

    results = table.find_records(conn.cursor(), statement, [maybe_flight["id"].inner])
    table.display_records(results)

# this is a query that joins flights and pilots via the flight_pilot junction table, joining on flight.id with
# flight_pilot.flight_id and flight_pilot.pilot_id on pilot.id respectively. Note that this query uses inner joins when joining
# flights to pilots via the flight_pilot junction table, thus ensuring that only pilots to which flights have been assigned 
# would be returned. Furthermore, this query filters the joined results by the given pilot_id, thus showing
# the flight information for the flights to which the selected pilot has been assigned.
# This query also sorts the results by departure time in ascending order, so as to prioritize the most imminent events
def pilot_schedule(conn: sqlite3.Connection):
    pilot_table = PilotTable()

    print("Please select a pilot: ")
    maybe_pilot = pilot_table.table_def.select_record(conn.cursor())

    if maybe_pilot is None:
        return
    
    statement = """
        SELECT
            f.flight_number,
            f.status,
            f.departure_time,
            f.arrival_time,
            origin.icao_code AS origin,
            destination.icao_code AS destination
        FROM
            flight AS f
        JOIN
            flight_pilot AS fp ON f.id = fp.flight_id
        JOIN
            pilot AS p ON fp.pilot_id = p.id
        JOIN
            airport AS origin ON f.origin_id = origin.id
        JOIN
            airport AS destination ON f.destination_id = destination.id
        WHERE
            p.id = ?
        ORDER BY
            f.departure_time
    """

    table = TableDef("pilot_schedule", [
        ColumnDef("flight_number", DataType.Text),
        ColumnDef("status", DataType.Text),
        ColumnDef("departure_time", DataType.DateTime),
        ColumnDef("arrival_time", DataType.DateTime),
        ColumnDef("origin", DataType.Text),
        ColumnDef("destination", DataType.Text)
    ])

    results = table.find_records(conn.cursor(), statement, [maybe_pilot["id"].inner])
    table.display_records(results)

# this is an aggregation query that counts the number of times each pilot visits each destination
# and groups the results by pilot and destination. At the end the results are sorted by visits in
# descending order. this query uses inner joins to join pilots and destinations via the flight_pilot junnction table
# and therefore does not produce rows where the pilot never visits a destination
def pilot_destination_frequencies(conn: sqlite3.Connection):
    statement = """
        SELECT
            p.name AS pilot,
            a.icao_code AS destination,
            COUNT(*) AS visits
        FROM
            flight_pilot fp
        JOIN
            flight f ON fp.flight_id = f.id
        JOIN
            pilot p ON fp.pilot_id = p.id
        JOIN
            airport a ON f.destination_id = a.id
        GROUP BY
            p.id, a.id
        ORDER BY
            visits DESC
    """
    
    table = TableDef("pilot_destination_frequencies", [
        ColumnDef("pilot", DataType.Text),
        ColumnDef("destination", DataType.Text),
        ColumnDef("visits", DataType.Int),
    ])

    results = table.find_records(conn.cursor(), statement, [])
    table.display_records(results)


# Define adapter for datetime -> string
def adapt_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# Define converter for string -> datetime
def convert_datetime(s: bytes) -> datetime:
    return datetime.strptime(s.decode("utf-8"), "%Y-%m-%d %H:%M:%S")

def test():
    # Register the datetime adapter and converter to suppress the deprecation warning
    sqlite3.register_adapter(datetime, adapt_datetime)
    sqlite3.register_converter("DATETIME", convert_datetime)

    with sqlite3.connect("airline.db") as conn:
        # use sqlite3.Row as row_factory to be able to access columns by name
        conn.row_factory = sqlite3.Row
        # enable foreign key support explicitly so that we can enforce foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON")

        # pilot_destination_frequencies(conn)
        # unassigned_pilots(conn)

        # flight_pilot_assignments(conn)
        pilot_schedule(conn)