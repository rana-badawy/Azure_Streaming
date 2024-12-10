import datetime
from faker import Faker

def get_data():
    fake = Faker()

    return {
        "temperature": fake.random_int(1, 100),
        "humidity": fake.random_int(1, 100),
        "city": fake.city(),
        "timestamp": fake.date_time(end_datetime=datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    }
