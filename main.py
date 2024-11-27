import os, platform

import sqlite3
import uuid

class Console:
    def __init__(self):
        self.conn = sqlite3.connect("airline.db")
        self.conn.row_factory = sqlite3.Row

        cursor = self.conn.cursor()

        cursor.execute(Destination.ddl())

    def run(self):
        while True:
            self.select_action()

            if self.do_more():
                continue
            
            return

    def select_action(self):  
        clear_and_print("Please choose an option :)")

        options = [
            {
                "name": "Create Destination",
                "endpoint": self.create_destination
            },
            {
                "name": "Lookup Destination",
                "endpoint": self.lookup_destination
            }
        ]

        for idx, option in enumerate(options):
            print(f"    {idx + 1}. {option["name"]}")

        while True:
            selected_idx = input("Please enter an option number to continue: ")

            try:
                selected_idx = int(selected_idx) - 1

                if selected_idx >= 0 and selected_idx < len(options):
                    break

                clear_and_print("Invalid selection. Please try again")
            except ValueError:
                clear_and_print("Invalid selection. Please try again")

        clear_stdout()
        option["endpoint"]()

    def lookup_destination(self):
        while True:
            id = input("Enter destination id: ")

            maybe_dst = Destination.get_by_id(self.conn.cursor(), id)
            if maybe_dst is None:
                if self.try_again("A destination by that id could not be found"):
                    continue
                return
            
            clear_and_print("Destination Details:")
            print(f"    id:     {maybe_dst.id}")
            print(f"    name:   {maybe_dst.name}") 
            return

    def create_destination(self):
        while True:
            name = input("Enter destination name: ")

            dst = Destination(name)

            try:
                dst.upsert(self.conn.cursor())
            except sqlite3.IntegrityError:
                if self.try_again("A destination by that name already exists"):
                    continue
                return
            except sqlite3.Error:
                print("An unrecoverable error occurred")

            break
        
        self.conn.commit()
        print(f"Successfully created destination with id: {dst.id}")

    def try_again(self, msg):
        try_again = input(f"{msg}. Would you like to try again? (Y/n): ")
        clear_stdout()
        if try_again in "Yy":
            return True
        return False
    
    def do_more(self):
        do_more = input("Is there anything else you would like to do today? (Y/n): ")
        if do_more in "Yy":
            return True
        return False

class Destination():
    def __init__(self, name):
        self.id = uuid.uuid4().hex
        self.name = name
        
    def upsert(self, cursor):
        command = """
            insert into destinations (id, name)
            values (:id, :name)
            on conflict (id)
            do update
            set name = :name
            where id = :id
        """

        cursor.execute(command, vars(self))
    
    @staticmethod
    def delete(cursor, id):
        command = """
            delete from destinations
            where id = ?
        """

        cursor.execute(command, id)
    
    @staticmethod
    def get_by_id(cursor, id):
        command = """
            select *
            from destinations
            where id = :id
        """

        row = cursor.execute(command, { "id": id }).fetchone()
        if row is None:
            return None
        
        return Destination.from_row(row)

    @staticmethod
    def from_row(row):
        val = Destination.__new__(Destination)
        val.id = row["id"]
        val.name = row["name"]

        return val

    @staticmethod
    def ddl():
        return """
            create table if not exists destinations (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
        """
    
class Pilot:
    def __init__(self, name):
        self.id = uuid.uuid4()
        self.name = name

    def upsert(self, cursor):
        command = """
            insert into pilots (id, name)
            values (:id, :name)
            on conflict (id)
            do update
            set name = :name
            where id = :id
        """

    def delete(cursor, id):
        command = """
            delete from pilots
            where id = :id
        """

    def ddl():
        return """
            create table if not exists pilots (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            )
        """

class Flight:
    def __init__(self, number, date, scheduled_departure, scheduled_arrival, origin_id, destination_id):
        self.id = uuid.uuid4()
        self.number = number
        # this is the date of the flight in utc
        self.date = date
        # this is the date and time of scheduled departure in utc
        self.scheduled_departure = scheduled_departure
        # this is the date and time of scheduled arrival in utc
        self.scheduled_arrival = scheduled_arrival
        self.origin_id = origin_id
        self.destination_id = destination_id
        self.pilot_id = None

    def upsert(self, cursor):
        command = """
            insert into flights (id, number, date, origin_id, destination_id, scheduled_departure, scheduled_arrival, pilot_id)
            values (:id, :number, :date, :scheduled_departure, :scheduled_arrival, :origin_id, :destination_id, :pilot_id)
            on conflict (id)
            do update
            set number = :number, date = :date, scheduled_departure = :scheduled_departure, scheduled_arrival = :scheduled_arrival, origin_id = :origin_id, destination_id = :destination_id, pilot_id = :pilot_id
            where id = :id
        """
    
    def delete(cursor, id):
        command = """
            delete from flights
            where id = :id
        """

    def ddl():
        return """
            create table if not exists flights (
                id TEXT PRIMARY KEY,
                number TEXT NOT NULL,
                date TEXT NOT NULL,
                scheduled_departure INTEGER,
                scheduled_arrival INTEGER,
                origin_id TEXT NOT NULL,
                destination_id TEXT NOT NULL,
                UNIQUE(number, date),
                FOREIGN KEY(origin_id) REFERENCES destinations(id),
                FOREIGN KEY(destination_id) REFERENCES destinations(id)
            )
        """

class FlightEvent:
    def __init__(self, flight_id, event_type, timestamp):
        self.id = uuid.uuid4()
        self.flight_id = flight_id
        self.event_type = event_type
        self.timestamp = timestamp

    def create(self, cursor):
        command = """
            INSERT INTO flight_events (id, flight_id, event_type, timestamp) VALUES (:id, :flight_id, :event_type, :timestamp)
        """

    def ddl():
        return """
            create table if not exists flight_events (
                id TEXT PRIMARY KEY,
                flight_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                FOREIGN KEY(flight_id) REFERENCES flights(id)
            )
        """

def clear_stdout():
   if platform.system() == 'Windows':
      os.system('cls')
   else:
      os.system('clear')

def clear_and_print(message):
    clear_stdout()
    print(message)

def clear_and_input(message):
    clear_stdout()
    return input(message)


def main():
    Console().run()
    # print(Destination.ddl())

if __name__ == "__main__":
    main()