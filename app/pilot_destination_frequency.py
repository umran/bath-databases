import sqlite3

# local imports
from .table import TableDef, ColumnDef, DataType

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

def pilot_schedule(conn: sqlite3.Connection):
    table = TableDef("pilot_schedule", [
        ColumnDef("flight_number", DataType.Text),
        ColumnDef("status", DataType.Text),
        ColumnDef("departure_time", DataType.DateTime),
        ColumnDef("arrival_time", DataType.DateTime),
        ColumnDef("origin", DataType.Text),
        ColumnDef("destination", DataType.Text)
    ])