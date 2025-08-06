SELECT
    e.employee_id,
    e.first_name,
    e.last_name,
    d.department_name,
    m.first_name AS manager_first_name,
    m.last_name AS manager_last_name,
    s.salary,
    s.bonus,
    s.salary + s.bonus AS total_compensation,
    p.project_name,
    p.start_date AS project_start,
    p.end_date AS project_end,
    CASE
        WHEN p.end_date IS NULL THEN 'Ongoing'
        WHEN p.end_date < CURRENT_DATE THEN 'Completed'
        ELSE 'In Progress'
    END AS project_status,
    COUNT(t.task_id) AS total_tasks,
    SUM(CASE WHEN t.status = 'Completed' THEN 1 ELSE 0 END) AS completed_tasks,
    ROUND(
        (SUM(CASE WHEN t.status = 'Completed' THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(t.task_id), 0),
        2
    ) AS completion_percentage
FROM
    {{ref("employees")}} e
    INNER JOIN departments d ON e.department_id = d.department_id
    LEFT JOIN employees m ON e.manager_id = m.employee_id
    INNER JOIN salaries s ON e.employee_id = s.employee_id
    LEFT JOIN project_assignments pa ON e.employee_id = pa.employee_id
    LEFT JOIN projects p ON pa.project_id = p.project_id
    LEFT JOIN tasks t ON p.project_id = t.project_id AND e.employee_id = t.assigned_to
WHERE
    e.status = 'Active'
    AND d.location IN ('New York', 'Chicago', 'San Francisco')
    AND s.effective_date = (
        SELECT MAX(s2.effective_date)
        FROM salaries s2
        WHERE s2.employee_id = e.employee_id
    )
    AND (p.start_date >= DATE_TRUNC('year', CURRENT_DATE) OR p.start_date IS NULL)
GROUP BY
    e.employee_id,
    e.first_name,
    e.last_name,
    d.department_name,
    m.first_name,
    m.last_name,
    s.salary,
    s.bonus,
    p.project_name,
    p.start_date,
    p.end_date
HAVING
    completion_percentage > 50 OR completion_percentage IS NULL
ORDER BY
    total_compensation DESC,
    e.last_name ASC,
    p.project_start DESC
LIMIT 100;