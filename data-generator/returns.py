import random
from datetime import date, timedelta

from db import get_connection


random.seed(42)


RETURN_REASONS = [
    "damaged",
    "wrong_item",
    "not_as_expected",
    "changed_mind",
    "late_delivery",
    "size_issue",
]

RETURN_STATUSES = [
    "requested",
    "approved",
    "received",
    "refunded",
]


def generate_returns(return_rate=0.08):
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    oi.order_item_id,
                    oi.quantity,
                    oi.unit_price,
                    oi.discount_pct,
                    o.order_date,
                    o.order_status
                FROM retail.order_items oi
                JOIN retail.orders o
                    ON oi.order_id = o.order_id
                LEFT JOIN retail.returns r
                    ON oi.order_item_id = r.order_item_id
                WHERE r.return_id IS NULL
                  AND o.order_status = 'delivered'
                """
            )

            rows = cursor.fetchall()

            inserted = 0

            for (
                order_item_id,
                quantity,
                unit_price,
                discount_pct,
                order_date,
                order_status,
            ) in rows:
                if random.random() > return_rate:
                    continue

                quantity_returned = random.randint(
                    1,
                    quantity,
                )

                net_unit_price = float(unit_price) * (
                    1 - float(discount_pct) / 100
                )

                refund_amount = round(
                    quantity_returned * net_unit_price,
                    2,
                )

                return_date = (
                    order_date.date()
                    + timedelta(
                        days=random.randint(1, 30)
                    )
                )

                cursor.execute(
                    """
                    INSERT INTO retail.returns (
                        order_item_id,
                        return_date,
                        return_reason,
                        quantity_returned,
                        refund_amount,
                        return_status
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        order_item_id,
                        return_date,
                        random.choice(RETURN_REASONS),
                        quantity_returned,
                        refund_amount,
                        random.choice(RETURN_STATUSES),
                    ),
                )

                inserted += 1

            connection.commit()

            print(
                f"Inserted {inserted:,} returns"
            )


if __name__ == "__main__":
    generate_returns()