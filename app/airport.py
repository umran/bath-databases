import sqlite3

# local imports
from .table import TableDef, ColumnDef, DataType
from .util import binary_decision

class AirportTable():
    table_def: TableDef

    def __init__(self):
        self.table_def = TableDef("airport", [
            ColumnDef("id", DataType.Int),
            ColumnDef("icao_code", DataType.Text),
            ColumnDef("name", DataType.Text),
            ColumnDef("city", DataType.Text)
        ])
    
    def create_record(self, conn: sqlite3.Connection):
        # get the values of all non-auto fields from user input
        values = self.table_def.get_column_values(lambda col: col.name not in ["id"])
        
        statement = f"""
            INSERT INTO airport
                (icao_code, name, city)
            VALUES
                (?, ?, ?) 
        """

        conn.execute(statement, [value.inner for value in values])
        conn.commit()

        print("new airport created successfully")

    def update_record(self, conn: sqlite3.Connection):
        # first, select a record to update
        print("Select an airport to update: ")
        record = self.table_def.select_natural_record(conn.cursor())

        if record is None:
            return

        updateable_columns = [col for col in self.table_def.columns if col.name not in ["id"]]

        for column in updateable_columns:
            if binary_decision(f"would you like to update {column.name}? "):
                val = self.table_def.get_value(column, f"Please enter the new value for {column.name}: ")
                record[column.name] = val
            
        print("the airport will be updated to reflect the following values:")
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

                print("existing airport updated successfully")
        

    def create_table(self, conn: sqlite3.Connection):
        statement = """
            CREATE TABLE IF NOT EXISTS airport (
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

    airport_table = AirportTable()
    airport_table.create_table(conn)

    airport_table.create_record(conn)

    # airport_table.select_natural_record(conn.cursor())

# test()
