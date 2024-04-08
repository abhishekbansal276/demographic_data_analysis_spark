import json
from faker import Faker
import random

fake = Faker("en_IN")

aadhaar_ids = [str(fake.random_number(digits=16)) for _ in range(1000)]

persons_data = []
for i in range(1000):
    person = {
        "aadhaar_id": aadhaar_ids[i],
        "person_name": {
            "first_name": fake.first_name(),
            "second_name": fake.last_name()
        },
        "gender": fake.random_element(elements=('Male', 'Female', 'Other')),
        "age": float(fake.random_int(min=18, max=90)),
        "dob": fake.date_of_birth(minimum_age=1, maximum_age=120).strftime('%Y-%m-%d'),
        "occupation": fake.random_element(elements=('Engineer', 'Doctor', 'Teacher', 'Business_Owner', 'Unemployed')),
        "monthly_income": float(fake.random_int(min=18000, max=9000000)),
        "phone": fake.phone_number()
    }
    if random.random() < 0.2:
        person["occupation"] = None
        person["monthly_income"] = None

    persons_data.append(person)

households_data = []
for i in range(1000):
    household = {
        "aadhaar_id": aadhaar_ids[i],
        "father_name": fake.name(),
        "address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state(),
            "postalCode": fake.postcode()
        },
        "monthly_income": float(fake.random_int(min=20000, max=1000000)),
        "category": fake.random_element(elements=('Middle Class', 'Upper Class', 'Lower Class')),
        "family_members": [fake.name() for _ in range(random.randint(2, 6))]
    }
    if random.random() < 0.2:
        household["monthly_income"] = None
        household["category"] = None

    households_data.append(household)

with open("persons.json", "w") as persons_file:
    for person in persons_data:
        json.dump(person, persons_file)
        persons_file.write(",\n")

with open("households.json", "w") as households_file:
    for household in households_data:
        json.dump(household, households_file)
        households_file.write(",\n")

print("JSON files created: persons.json, households.json")
