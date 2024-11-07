class Destination:
    def __init__(self, name):
        self.name = name

    def reconcile_db(self):
        command = """
            insert into destinations (id, name)
            values (:id, :name)
            on conflict (id)
            do update
            set name = :name
            where id = :id
        """

class Pilot:
    def __init__(self, name):
        self.name = name

    def reconcile_db(self):
        command = """
            insert into pilots (id, name)
            values (:id, :name)
            on conflict (id)
            do update
            set name = :name
            where id = :id
        """

class Flight:
    def __init__(self, number, origin_id, destination_id, scheduled_departure, scheduled_arrival):
        self.number = number
        self.origin_id = origin_id
        self.destination_id = destination_id
        self.scheduled_departure = scheduled_departure
        self.scheduled_arrival = scheduled_arrival

    def reconcile_db(self):
        command = """
            insert into flights (id, number, origin_id, destination_id, scheduled_departure, scheduled_arrival)
            values (:id, :number, :origin_id, :destination_id, :scheduled_departure, :scheduled_arrival)
            on conflict (id)
            do update
            set number = :number, origin_id = :origin_id, destination_id = :destination_id, scheduled_departure = :scheduled_departure, scheduled_arrival = :scheduled_arrival
            where id = :id
        """

class FlightEvent:
    def __init__(self, flight_id, event_name, timestamp):
        self.flight_id = flight_id
        self.event_name = event_name
        self.timestamp = timestamp