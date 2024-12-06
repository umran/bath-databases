import sqlite3

# local imports
from .table import TableDef, ColumnDef, DataType
from .airport import AirportTable
from .util import binary_decision

class PilotTable():
    table_def: TableDef

    def __init__(self):
        self.table_def = TableDef("pilot", [
            ColumnDef("id", DataType.Int),
            ColumnDef("name", DataType.Text),
            ColumnDef("logged_hours", DataType.Int),
            ColumnDef("home_airport_id", DataType.Int)
        ])
    
    def create_record(self, conn: sqlite3.Connection):
        # create a handle to airport table since the user will have to select
        # an airport when entering home_airport_id
        airport_table = AirportTable()

        # get the values of all non-auto fields from user input
        values = self.table_def.get_column_values(lambda col: col.name not in ["id", "home_airport_id"])

        print("Please take a moment to select the home airport")
        maybe_home_airport = airport_table.table_def.select_record_from_db(conn.cursor())
        if maybe_home_airport is None:
            return

        values.append(maybe_home_airport["id"])

        statement = f"""
            INSERT INTO pilot
                (name, logged_hours, home_airport_id)
            VALUES
                (?, ?, ?) 
        """

        conn.execute(statement, [value.inner for value in values])
        conn.commit()

    def update_record(self, conn: sqlite3.Connection):
        # first, select a record to update
        print("Select a pilot to update: ")
        record = self.table_def.select_record_from_db(conn.cursor())

        if record is None:
            return

        updateable_columns = [col for col in self.table_def.columns if col.name not in ["id"]]

        # get a handle to the airport table representation so that we can display airports in
        # case the user wants to update the origin_id and destination_id
        airport_table = AirportTable()

        for column in updateable_columns:
            if binary_decision(f"would you like to update {column.name}? "):
                if column.name == "home_airport_id":
                    print(f"Please find an airport to set as the new {column.name}")
                    val = airport_table.table_def.select_record_from_db(conn.cursor())
                    if val is None:
                        continue
                    else:
                        record[column.name] = val["id"]
                else:
                    val = self.table_def.get_value(column, f"Please enter the new value for {column.name}: ")
                    record[column.name] = val
            
        print("the pilot will be updated to reflect the following values:")
        for key in record.keys():
            value = record.get(key)
            value_str = ""
            if value is None:
                value_str = "NULL"
            else:
                value_str = value.to_str()

            print(f"    {key}: {value_str}")
        
        if binary_decision("would you like to proceed with these changes?"):
            update_set = [f"{column.name} = ?" for column in updateable_columns]
            values = [record[column.name].inner for column in updateable_columns]

            if len(update_set) > 0:
                statement = f"""
                    UPDATE {self.table_def.name} SET {", ".join(update_set)} WHERE id = ?
                """

                # debugging
                print(statement)

                values.append(record["id"].inner)
                conn.execute(statement, values)
                conn.commit()
        

    def create_table(self, conn: sqlite3.Connection):
        statement = """
            CREATE TABLE IF NOT EXISTS pilot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                logged_hours INTEGER NOT NULL,
                home_airport_id INTEGER NOT NULL,
                FOREIGN KEY (home_airport_id) REFERENCES airport(id)
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

    pilot_table = PilotTable()
    pilot_table.create_table(conn)

    pilot_table.update_record(conn)

    # pilot_table.select_record(conn.cursor())

test()
