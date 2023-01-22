WITH cte_ordored_data AS(
	SELECT
		contact_id,
		store_id,
		created_at
	FROM orders
	WHERE
	-- filter scope
		created_at BETWEEN created_at + MAKE_INTERVAL(-3,0,0,0) AND CURRENT_TIMESTAMP
)
SELECT 
	u_c.contact_id,
	u_scope_res.store_id AS favorite_store_id,
	u_wd_res.store_id AS weekdays_favorite_store_id,
	u_we_res.store_id AS weekends_favorite_store_id
FROM (SELECT DISTINCT contact_id FROM cte_ordored_data ORDER BY contact_id ASC) AS u_c

-- the max(created_at) will order the partition by most recent orders by the customer if the count is same

-- favorite store per customer in the define scope, 

LEFT JOIN 
	(
		SELECT 
			*
		FROM
		(
			SELECT
				contact_id,
				store_id,
				ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY count(*) DESC, MAX(created_at) DESC) as r_n
			
			FROM cte_ordored_data
			GROUP BY contact_id, store_id
			
		) AS scope_res
		WHERE scope_res.r_n = 1
	 ) AS u_scope_res
ON u_scope_res.contact_id = u_c.contact_id
-- favorite store per customer in the define scope for only weekdays
LEFT JOIN 
	(
		SELECT 
			*
		FROM
		(
			SELECT
				contact_id,
				store_id,
				ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY count(*) DESC, MAX(created_at) DESC) as r_n
			FROM cte_ordored_data
			WHERE DATE_PART('dow', created_at) in (1,2,3,4,5)
			GROUP BY contact_id, store_id
		) as wd_res
		WHERE wd_res.r_n = 1
	 ) AS u_wd_res
ON u_wd_res.contact_id = u_c.contact_id
-- favorite store per customer in the define scope for only weekends
LEFT JOIN 
	(
		SELECT 
			*
		FROM
		(
			SELECT
				contact_id,
				store_id,
				ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY count(*) DESC, MAX(created_at) DESC) as r_n
			FROM cte_ordored_data
			WHERE DATE_PART('dow', created_at) in (6,0)
			GROUP BY contact_id, store_id
		) as we_res
		WHERE we_res.r_n = 1
	 ) AS u_we_res
ON u_we_res.contact_id = u_c.contact_id