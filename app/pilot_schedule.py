import sqlite3

# local imports
from .table import TableDef, ColumnDef, DataType

query = """
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

def pilot_schedule(conn: sqlite3.Connection):
    table = TableDef("pilot_schedule", [
        ColumnDef("flight_number", DataType.Text),
        ColumnDef("status", DataType.Text),
        ColumnDef("departure_time", DataType.DateTime),
        ColumnDef("arrival_time", DataType.DateTime),
        ColumnDef("origin", DataType.Text),
        ColumnDef("destination", DataType.Text)
    ])