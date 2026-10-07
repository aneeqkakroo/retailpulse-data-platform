import random
from datetime import date, timedelta

from faker import Faker

from db import get_connection


fake = Faker("en_GB")

Faker.seed(42)
random.seed(42)


CUSTOMER_SEGMENTS = [
    "new",
    "regular",
    "loyal",
    "high_value",
]


def random_signup_date():
    start = date(2022, 1, 1)
    days = (date.today() - start).days

    return start + timedelta(
        days=random.randint(0, days)
    )


def generate_customer():
    first_name = fake.first_name()
    last_name = fake.last_name()

    unique_number = random.randint(
        1_000_000,
        9_999_999
    )

    email = (
        f"{first_name}.{last_name}."
        f"{unique_number}@example.com"
    ).lower()

    return (
        first_name,
        last_name,
        email,
        fake.date_of_birth(
            minimum_age=18,
            maximum_age=85,
        ),
        fake.street_address(),
        fake.city(),
        fake.county(),
        fake.postcode(),
        "United Kingdom",
        random.choice(CUSTOMER_SEGMENTS),
        random_signup_date(),
    )


def generate_customers(number_of_customers):
    sql = """
        INSERT INTO retail.customers (
            first_name,
            last_name,
            email,
            date_of_birth,
            address_line_1,
            city,
            region,
            postcode,
            country,
            customer_segment,
            signup_date
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
    """

    batch_size = 1000

    with get_connection() as connection:
        with connection.cursor() as cursor:
            for start in range(
                0,
                number_of_customers,
                batch_size,
            ):
                remaining = number_of_customers - start

                current_batch = min(
                    batch_size,
                    remaining,
                )

                customers = [
                    generate_customer()
                    for _ in range(current_batch)
                ]

                cursor.executemany(
                    sql,
                    customers,
                )

                connection.commit()

                print(
                    f"Inserted "
                    f"{start + current_batch:,} "
                    f"customers"
                )


if __name__ == "__main__":
    generate_customers(10_000)