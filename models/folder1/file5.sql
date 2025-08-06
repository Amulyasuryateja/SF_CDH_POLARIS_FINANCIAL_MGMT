SELECT
    p.product_id,
    p.product_name,
    p.stock_quantity,
    p.reorder_level,
    s.supplier_name,
    CASE
        WHEN p.stock_quantity < p.reorder_level THEN 'Restock Needed'
        ELSE 'Sufficient Stock'
    END AS restock_status

    from {{ ref("dim_material") }}
    JOIN suppliers s ON p.supplier_id = s.supplier_id
WHERE
    p.active = TRUE
ORDER BY
    restock_status, p.stock_quantity ASC;
 
 