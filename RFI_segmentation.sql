WITH cte_contact_last_avg_order AS (
	SELECT 
		t.contact_id,
		EXTRACT('DAY' FROM CURRENT_TIMESTAMP - MAX(t.created_at)) AS time_since_last_order,
		AVG(t.diff) AS avg_delay_between_orders
	FROM
	(
		SELECT 
			contact_id,
			created_at,
			COALESCE(EXTRACT('DAY' FROM created_at - LAG(created_at, 1) OVER(partition by contact_id order by created_at)), 0) AS diff 
		FROM orders
	) AS t
	WHERE 
		t.diff > 0 -- filter first time purchase and varition time less than one day.
	GROUP BY
		t.contact_id
	HAVING 
		COUNT(*) > 2 -- filter those who have less than 3 orders, '2' because there will be 2 variation time between 3 orders.
)
SELECT 
	contact_id,
	-- case for rfi_label and thru
	CASE
		WHEN time_since_last_order/avg_delay_between_orders < 0.60
		THEN 'Recent purchase'
		WHEN time_since_last_order/avg_delay_between_orders < 1
		THEN 'Soon to be reached'
		WHEN time_since_last_order/avg_delay_between_orders < 1.5
		THEN 'Overdue'
		WHEN time_since_last_order/avg_delay_between_orders < 2.5
		THEN 'Significantly Overdue'
		WHEN time_since_last_order/avg_delay_between_orders >= 2.5
		THEN 'Inactive'
	END AS rfi_label,
	
	ROUND(time_since_last_order, 2) AS time_since_last_order,
	ROUND(avg_delay_between_orders, 2) AS avg_delay_between_orders,
	ROUND(time_since_last_order/avg_delay_between_orders) * 100 AS purchasing_rythm -- * 100 for percentage
FROM cte_contact_last_avg_order
