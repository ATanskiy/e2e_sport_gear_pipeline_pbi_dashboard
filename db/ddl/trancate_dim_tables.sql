-- Thruncate dimension tables
TRUNCATE TABLE
    prod.products,
    prod.product_subcategories,
    prod.product_categories,
    prod.customers,
    prod.stores,
    prod.employees,
    prod.payment_methods,
    prod.shipping_methods,
    playground.products,
    playground.product_subcategories,
    playground.product_categories,
    playground.customers,
    playground.stores,
    playground.employees,
    playground.payment_methods,
    playground.shipping_methods
RESTART IDENTITY CASCADE;