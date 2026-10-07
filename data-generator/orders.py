import random
from datetime import datetime, timedelta

from db import get_connection


random.seed(42)


ORDER_STATUSES = [
    ("delivered", 0.72),
    ("shipped", 0.08),
    ("processing", 0.06),
    ("pending", 0.04),
    ("cancelled", 0.10),
]

PAYMENT_METHODS = [
    ("card", 0.70),
    ("paypal", 0.18),
    ("apple_pay", 0.08),
    ("google_pay", 0.04),
]


def weighted_choice(options):
    values = [item[0] for item in options]
    weights = [item[1] for item in options]

    return random.choices(
        values,
        weights=weights,
        k=1,
    )[0]


def random_order_date():
    start = datetime(2023, 1, 1)
    end = datetime.now()

    seconds = int(
        (end - start).total_seconds()
    )

    return start + timedelta(
        seconds=random.randint(0, seconds)
    )


def load_customer_ids(cursor):
    cursor.execute(
        """
        SELECT customer_id
        FROM retail.customers
        ORDER BY customer_id
        """
    )

    return [
        row[0]
        for row in cursor.fetchall()
    ]


def load_products(cursor):
    cursor.execute(
        """
        SELECT
            product_id,
            retail_price
        FROM retail.products
        WHERE active = TRUE
        ORDER BY product_id
        """
    )

    return cursor.fetchall()


def choose_customer(customer_ids):
    # Creates some repeat/high-frequency customers.
    if random.random() < 0.20:
        pool_size = max(
            1,
            int(len(customer_ids) * 0.10),
        )

        return random.choice(
            customer_ids[:pool_size]
        )

    return random.choice(customer_ids)


def choose_products(
    products,
    number_of_items,
):
    # Lower product IDs are intentionally somewhat
    # more popular to simulate a long-tail catalogue.

    selected = []

    for _ in range(number_of_items):
        if random.random() < 0.60:
            popular_pool_size = max(
                1,
                int(len(products) * 0.20),
            )

            product = random.choice(
                products[:popular_pool_size]
            )
        else:
            product = random.choice(products)

        selected.append(product)

    return selected


def calculate_discount():
    probability = random.random()

    if probability < 0.70:
        return 0

    if probability < 0.85:
        return 5

    if probability < 0.95:
        return 10

    return 20


def create_order(
    cursor,
    customer_id,
):
    order_date = random_order_date()

    order_status = weighted_choice(
        ORDER_STATUSES
    )

    payment_method = weighted_choice(
        PAYMENT_METHODS
    )

    shipping_cost = random.choices(
        [0.00, 3.99, 5.99],
        weights=[0.55, 0.35, 0.10],
        k=1,
    )[0]

    cursor.execute(
        """
        SELECT
            city,
            region,
            postcode
        FROM retail.customers
        WHERE customer_id = %s
        """,
        (customer_id,),
    )

    city, region, postcode = cursor.fetchone()

    cursor.execute(
        """
        INSERT INTO retail.orders (
            customer_id,
            order_date,
            order_status,
            payment_method,
            shipping_city,
            shipping_region,
            shipping_postcode,
            shipping_cost
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        RETURNING order_id
        """,
        (
            customer_id,
            order_date,
            order_status,
            payment_method,
            city,
            region,
            postcode,
            shipping_cost,
        ),
    )

    return cursor.fetchone()[0]


def create_order_items(
    cursor,
    order_id,
    products,
):
    number_of_items = random.choices(
        [1, 2, 3, 4, 5],
        weights=[0.42, 0.30, 0.16, 0.08, 0.04],
        k=1,
    )[0]

    selected_products = choose_products(
        products,
        number_of_items,
    )

    rows = []

    for product_id, retail_price in selected_products:
        quantity = random.choices(
            [1, 2, 3, 4],
            weights=[0.74, 0.18, 0.06, 0.02],
            k=1,
        )[0]

        discount_pct = calculate_discount()

        rows.append(
            (
                order_id,
                product_id,
                quantity,
                retail_price,
                discount_pct,
            )
        )

    cursor.executemany(
        """
        INSERT INTO retail.order_items (
            order_id,
            product_id,
            quantity,
            unit_price,
            discount_pct
        )
        VALUES (
            %s, %s, %s, %s, %s
        )
        """,
        rows,
    )


def generate_orders(
    number_of_orders,
):
    batch_size = 1000

    with get_connection() as connection:
        with connection.cursor() as cursor:
            customer_ids = load_customer_ids(
                cursor
            )

            products = load_products(
                cursor
            )

            if not customer_ids:
                raise RuntimeError(
                    "No customers found."
                )

            if not products:
                raise RuntimeError(
                    "No active products found."
                )

            for order_number in range(
                1,
                number_of_orders + 1,
            ):
                customer_id = choose_customer(
                    customer_ids
                )

                order_id = create_order(
                    cursor,
                    customer_id,
                )

                create_order_items(
                    cursor,
                    order_id,
                    products,
                )

                if (
                    order_number % batch_size == 0
                    or order_number
                    == number_of_orders
                ):
                    connection.commit()

                    print(
                        f"Inserted "
                        f"{order_number:,} "
                        f"orders"
                    )


if __name__ == "__main__":
    generate_orders(50_000)