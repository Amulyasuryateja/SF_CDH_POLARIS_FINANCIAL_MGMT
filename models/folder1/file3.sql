SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders,
    SUM(oi.unit_price * oi.quantity) AS lifetime_value,
    MAX(o.order_date) AS last_order_date
from {{source("sap", "customers")}} c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY
    c.customer_id, c.first_name, c.last_name
ORDER BY
    lifetime_value DESC
LIMIT 100;