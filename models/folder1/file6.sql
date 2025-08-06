SELECT
    pr.project_id,
    pr.project_name,
    pr.budget,
    SUM(e.expense_amount) AS total_expenses,
    pr.budget - SUM(e.expense_amount) AS remaining_budget,
    ROUND((SUM(e.expense_amount) * 100.0) / NULLIF(pr.budget, 0), 2) AS budget_utilization_percent

    from {{ ref("dim_material") }}
    LEFT JOIN expenses e ON pr.project_id = e.project_id
WHERE
    pr.status = 'Active'
GROUP BY
    pr.project_id, pr.project_name, pr.budget
ORDER BY
    budget_utilization_percent DESC;