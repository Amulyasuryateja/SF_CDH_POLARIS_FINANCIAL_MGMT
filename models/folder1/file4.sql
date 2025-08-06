SELECT
    r.region_name,
    c.category_name,
    EXTRACT(YEAR FROM o.order_date) AS order_year,
    EXTRACT(QUARTER FROM o.order_date) AS order_quarter,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.unit_price * oi.quantity) AS total_sales,
    AVG(oi.unit_price) AS avg_unit_price,
    MAX(oi.unit_price) AS max_unit_price,
    MIN(oi.unit_price) AS min_unit_price,
    COUNT(DISTINCT cu.customer_id) AS unique_customers,
    SUM(CASE WHEN o.status = 'Shipped' THEN 1 ELSE 0 END) AS shipped_orders,
    SUM(CASE WHEN o.status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders
from {{ ref("dim_material") }} o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
    INNER JOIN categories c ON p.category_id = c.category_id
    INNER JOIN regions r ON o.region_id = r.region_id
    INNER JOIN customers cu ON o.customer_id = cu.customer_id
WHERE
    o.order_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '2 year'
    AND r.region_name IN ('North', 'South', 'East', 'West')
GROUP BY
    r.region_name,
    c.category_name,
    EXTRACT(YEAR FROM o.order_date),
    EXTRACT(QUARTER FROM o.order_date)
HAVING
    SUM(oi.unit_price * oi.quantity) > 50000
ORDER BY
    r.region_name,
    c.category_name,
    order_year DESC,
    order_quarter DESC;
 
