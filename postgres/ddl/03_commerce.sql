-- Operational schema -- orders (header) + order_items (child)
-- Energy Commerce and Retail Media Analytics Platform
-- Author: Sharique Mohammad
-- Date: August 2026
--
-- orders.items_subtotal_eur is the sum of the order's order_items.line_total_eur
-- (a cross-row invariant guaranteed by the generator, not enforceable as a CHECK).
-- The same-row totals are enforced.

SET search_path = operational, public;

CREATE TABLE operational.orders (
    order_id            uuid          PRIMARY KEY,
    order_number        varchar(16)   NOT NULL UNIQUE,
    customer_id         uuid          NOT NULL REFERENCES operational.customers (customer_id),
    order_status        varchar(20)   NOT NULL DEFAULT 'placed'
                        CHECK (order_status IN
                               ('placed', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded')),
    ordered_at          timestamptz   NOT NULL,
    currency            char(3)       NOT NULL DEFAULT 'EUR' CHECK (currency = 'EUR'),
    items_subtotal_eur  numeric(12,2) NOT NULL CHECK (items_subtotal_eur >= 0),
    shipping_fee_eur    numeric(8,2)  NOT NULL DEFAULT 0 CHECK (shipping_fee_eur >= 0),
    total_eur           numeric(12,2) NOT NULL CHECK (total_eur >= 0),
    created_at          timestamptz   NOT NULL,
    updated_at          timestamptz   NOT NULL,
    CHECK (total_eur = items_subtotal_eur + shipping_fee_eur)
);

CREATE TABLE operational.order_items (
    order_item_id   uuid          PRIMARY KEY,
    order_id        uuid          NOT NULL
                    REFERENCES operational.orders (order_id) ON DELETE CASCADE,
    product_id      uuid          NOT NULL REFERENCES operational.products (product_id),
    quantity        integer       NOT NULL CHECK (quantity > 0),
    unit_price_eur  numeric(10,2) NOT NULL CHECK (unit_price_eur > 0),
    line_total_eur  numeric(12,2) NOT NULL CHECK (line_total_eur >= 0),
    created_at      timestamptz   NOT NULL,
    UNIQUE (order_id, product_id),
    CHECK (line_total_eur = round(unit_price_eur * quantity, 2))
);
