-- ==========================================================
-- RetailPulse Transactional Source Database
-- ==========================================================

CREATE SCHEMA IF NOT EXISTS retail;

SET search_path TO retail;


-- ==========================================================
-- CUSTOMERS
-- ==========================================================

CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,

    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,

    date_of_birth DATE,

    address_line_1 VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(100),
    postcode VARCHAR(20),
    country VARCHAR(50) DEFAULT 'United Kingdom',

    customer_segment VARCHAR(50),

    signup_date DATE NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ==========================================================
-- PRODUCTS
-- ==========================================================

CREATE TABLE products (
    product_id BIGSERIAL PRIMARY KEY,

    sku VARCHAR(50) NOT NULL UNIQUE,

    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    brand VARCHAR(100),

    supplier_id BIGINT,

    unit_cost NUMERIC(10,2) NOT NULL,
    retail_price NUMERIC(10,2) NOT NULL,

    active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_product_cost
        CHECK (unit_cost >= 0),

    CONSTRAINT chk_product_price
        CHECK (retail_price >= 0)
);


-- ==========================================================
-- ORDERS
-- ==========================================================

CREATE TABLE orders (
    order_id BIGSERIAL PRIMARY KEY,

    customer_id BIGINT NOT NULL
        REFERENCES customers(customer_id),

    order_date TIMESTAMP NOT NULL,

    order_status VARCHAR(30) NOT NULL,

    payment_method VARCHAR(30),

    shipping_city VARCHAR(100),
    shipping_region VARCHAR(100),
    shipping_postcode VARCHAR(20),

    shipping_cost NUMERIC(10,2) DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_order_status CHECK (
        order_status IN (
            'pending',
            'processing',
            'shipped',
            'delivered',
            'cancelled'
        )
    )
);


-- ==========================================================
-- ORDER ITEMS
-- ==========================================================

CREATE TABLE order_items (
    order_item_id BIGSERIAL PRIMARY KEY,

    order_id BIGINT NOT NULL
        REFERENCES orders(order_id),

    product_id BIGINT NOT NULL
        REFERENCES products(product_id),

    quantity INTEGER NOT NULL,

    unit_price NUMERIC(10,2) NOT NULL,

    discount_pct NUMERIC(5,2) DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_quantity
        CHECK (quantity > 0),

    CONSTRAINT chk_unit_price
        CHECK (unit_price >= 0),

    CONSTRAINT chk_discount
        CHECK (
            discount_pct >= 0
            AND discount_pct <= 100
        )
);


-- ==========================================================
-- RETURNS
-- ==========================================================

CREATE TABLE returns (
    return_id BIGSERIAL PRIMARY KEY,

    order_item_id BIGINT NOT NULL
        REFERENCES order_items(order_item_id),

    return_date DATE NOT NULL,

    return_reason VARCHAR(100),

    quantity_returned INTEGER NOT NULL,

    refund_amount NUMERIC(10,2),

    return_status VARCHAR(30),

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_return_quantity
        CHECK (quantity_returned > 0),

    CONSTRAINT chk_refund_amount
        CHECK (refund_amount >= 0)
);


-- ==========================================================
-- INDEXES
-- ==========================================================

CREATE INDEX idx_customers_updated_at
    ON customers(updated_at);

CREATE INDEX idx_products_updated_at
    ON products(updated_at);

CREATE INDEX idx_orders_customer_id
    ON orders(customer_id);

CREATE INDEX idx_orders_order_date
    ON orders(order_date);

CREATE INDEX idx_orders_updated_at
    ON orders(updated_at);

CREATE INDEX idx_order_items_order_id
    ON order_items(order_id);

CREATE INDEX idx_order_items_product_id
    ON order_items(product_id);

CREATE INDEX idx_order_items_updated_at
    ON order_items(updated_at);

CREATE INDEX idx_returns_updated_at
    ON returns(updated_at);


-- ==========================================================
-- UPDATED_AT TRIGGER
-- ==========================================================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN

    NEW.updated_at = CURRENT_TIMESTAMP;

    RETURN NEW;

END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON products
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


CREATE TRIGGER trg_order_items_updated_at
BEFORE UPDATE ON order_items
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


CREATE TRIGGER trg_returns_updated_at
BEFORE UPDATE ON returns
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();