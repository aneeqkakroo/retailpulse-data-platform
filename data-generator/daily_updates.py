import random
from datetime import datetime

from db import get_connection
from orders import (
    load_customer_ids,
    load_products,
    choose_customer,
    create_order_items,
    weighted_choice,
    PAYMENT_METHODS,
)


random.seed()


def generate_daily_orders(
    number_of_orders=500,
):
    with get_connection() as connection:
        with connection.cursor() as cursor:
            customer_ids = load_customer_ids(
                cursor
            )

            products = load_products(
                cursor
            )

            for _ in range(number_of_orders):
                customer_id = choose_customer(
                    customer_ids
                )

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

                city, region, postcode = (
                    cursor.fetchone()
                )

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
                        datetime.now(),
                        "pending",
                        weighted_choice(
                            PAYMENT_METHODS
                        ),
                        city,
                        region,
                        postcode,
                        random.choice(
                            [0.00, 3.99, 5.99]
                        ),
                    ),
                )

                order_id = cursor.fetchone()[0]

                create_order_items(
                    cursor,
                    order_id,
                    products,
                )

            connection.commit()

            print(
                f"Inserted "
                f"{number_of_orders:,} "
                f"new daily orders"
            )


def update_order_statuses():
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE retail.orders
                SET order_status = 'processing'
                WHERE order_status = 'pending'
                  AND order_date
                      < CURRENT_TIMESTAMP
                        - INTERVAL '1 hour'
                """
            )

            pending_to_processing = (
                cursor.rowcount
            )

            cursor.execute(
                """
                UPDATE retail.orders
                SET order_status = 'shipped'
                WHERE order_status = 'processing'
                  AND order_date
                      < CURRENT_TIMESTAMP
                        - INTERVAL '1 day'
                """
            )

            processing_to_shipped = (
                cursor.rowcount
            )

            cursor.execute(
                """
                UPDATE retail.orders
                SET order_status = 'delivered'
                WHERE order_status = 'shipped'
                  AND order_date
                      < CURRENT_TIMESTAMP
                        - INTERVAL '3 days'
                """
            )

            shipped_to_delivered = (
                cursor.rowcount
            )

            connection.commit()

            print(
                "Updated statuses:"
            )

            print(
                f"pending → processing: "
                f"{pending_to_processing:,}"
            )

            print(
                f"processing → shipped: "
                f"{processing_to_shipped:,}"
            )

            print(
                f"shipped → delivered: "
                f"{shipped_to_delivered:,}"
            )


def update_customers(
    number_of_customers=50,
):
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT customer_id
                FROM retail.customers
                ORDER BY RANDOM()
                LIMIT %s
                """,
                (number_of_customers,),
            )

            customer_ids = [
                row[0]
                for row in cursor.fetchall()
            ]

            for customer_id in customer_ids:
                new_segment = random.choice(
                    [
                        "regular",
                        "loyal",
                        "high_value",
                    ]
                )

                cursor.execute(
                    """
                    UPDATE retail.customers
                    SET customer_segment = %s
                    WHERE customer_id = %s
                    """,
                    (
                        new_segment,
                        customer_id,
                    ),
                )

            connection.commit()

            print(
                f"Updated "
                f"{len(customer_ids):,} "
                f"customers"
            )


if __name__ == "__main__":
    generate_daily_orders(500)
    update_order_statuses()
    update_customers(50)