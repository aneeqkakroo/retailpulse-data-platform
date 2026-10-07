TABLE_CONFIG = {
    "customers": {
        "source_table": "retail.customers",
        "watermark_column": "updated_at",
    },
    "products": {
        "source_table": "retail.products",
        "watermark_column": "updated_at",
    },
    "orders": {
        "source_table": "retail.orders",
        "watermark_column": "updated_at",
    },
    "order_items": {
        "source_table": "retail.order_items",
        "watermark_column": "updated_at",
    },
    "returns": {
        "source_table": "retail.returns",
        "watermark_column": "updated_at",
    },
}