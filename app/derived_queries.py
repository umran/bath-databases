import sqlite3

# local imports
from .table import TableDef, ColumnDef, DataType

# this is a query that joins flights and pilots via the flight_pilot junction table, joining on flight.id with
# flight_pilot.flight_id and flight_pilot.pilot_id on pilot.id respectively. This query filters the joined results by
# the given pilot_id, thus showing the flight information for the flights to which the selected pilot has been assigned
def pilot_schedule(cursor: sqlite3.Cursor):
    statement = """
        SELECT
            f.flight_number,
            f.status,
            f.departure_time,
            f.arrival_time,
            origin.icao_number AS origin,
            destination.icao_number AS destination,
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
            f.departure_time;
    """

    table = TableDef("pilot_schedule", [
        ColumnDef("flight_number", DataType.Text),
        ColumnDef("status", DataType.Text),
        ColumnDef("departure_time", DataType.DateTime),
        ColumnDef("arrival_time", DataType.DateTime),
        ColumnDef("origin", DataType.Text),
        ColumnDef("destination", DataType.Text)
    ])

# this is an aggregation query that counts the number of times each pilot visits each destination
# and groups the results by pilot and destination. At the end the sorts results are sorted by visits in
# descending order
def pilot_destination_frequencies(cursor: sqlite3.Cursor):
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
            airport a ON f.destination_airport_id = a.id
        GROUP BY
            p.id, a.id
        ORDER BY
            visits DESC
    """
    
    table = TableDef("pilot_destination_frequencies", [
        ColumnDef("pilot", DataType.Text),
        ColumnDef("destination", DataType.Text, nullable=True),
        ColumnDef("visits", DataType.Int),
    ])