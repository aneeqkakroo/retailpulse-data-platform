import random

from faker import Faker

from db import get_connection


fake = Faker("en_GB")

Faker.seed(42)
random.seed(42)


PRODUCT_CATALOGUE = {
    "Bedding": [
        "Microfibre Duvet",
        "Mattress Topper",
        "Pillow Pair",
        "Fitted Sheet",
        "Duvet Cover Set",
    ],
    "Bathroom": [
        "Bath Towel Set",
        "Bath Mat",
        "Shower Curtain",
        "Hand Towel Set",
        "Bathroom Storage Basket",
    ],
    "Kitchen": [
        "Cookware Set",
        "Cutlery Set",
        "Storage Container Set",
        "Kitchen Towels",
        "Chopping Board",
    ],
    "Living Room": [
        "Cushion",
        "Throw Blanket",
        "Table Lamp",
        "Storage Ottoman",
        "Curtains",
    ],
    "Home Office": [
        "Desk Lamp",
        "Laptop Stand",
        "Desk Organiser",
        "Office Chair Cushion",
        "Cable Management Box",
    ],
}


BRANDS = [
    "North & Home",
    "CasaLiving",
    "HavenWorks",
    "UrbanNest",
    "Oak & Linen",
    "Homestead",
]


def generate_product(product_number):
    category = random.choice(
        list(PRODUCT_CATALOGUE.keys())
    )

    base_name = random.choice(
        PRODUCT_CATALOGUE[category]
    )

    brand = random.choice(BRANDS)

    unit_cost = round(
        random.uniform(3.00, 80.00),
        2,
    )

    markup = random.uniform(
        1.35,
        2.50,
    )

    retail_price = round(
        unit_cost * markup,
        2,
    )

    sku = f"RP-{product_number:06d}"

    product_name = (
        f"{brand} {base_name}"
    )

    supplier_id = random.randint(
        1,
        100,
    )

    active = random.random() > 0.03

    return (
        sku,
        product_name,
        category,
        brand,
        supplier_id,
        unit_cost,
        retail_price,
        active,
    )


def generate_products(number_of_products):
    sql = """
        INSERT INTO retail.products (
            sku,
            product_name,
            category,
            brand,
            supplier_id,
            unit_cost,
            retail_price,
            active
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
    """

    batch_size = 1000

    with get_connection() as connection:
        with connection.cursor() as cursor:
            for start in range(
                0,
                number_of_products,
                batch_size,
            ):
                remaining = (
                    number_of_products - start
                )

                current_batch = min(
                    batch_size,
                    remaining,
                )

                products = [
                    generate_product(start + i + 1)
                    for i in range(current_batch)
                ]

                cursor.executemany(
                    sql,
                    products,
                )

                connection.commit()

                print(
                    f"Inserted "
                    f"{start + current_batch:,} "
                    f"products"
                )


if __name__ == "__main__":
    generate_products(5_000)