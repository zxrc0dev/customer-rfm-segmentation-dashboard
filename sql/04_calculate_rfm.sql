WITH cleaned AS (
    SELECT 
        customer_id,
        invoice,
        CAST(invoice_date AS TIMESTAMP) as invoice_date,
        quantity,
        price
    FROM public.retail_records
    WHERE quantity > 0
      AND price > 0
      AND customer_id IS NOT NULL
),
rfm_base AS (
    SELECT
        customer_id,
        MAX(invoice_date) AS last_purchase_date,
        COUNT(DISTINCT invoice) AS frequency,
        SUM(quantity * price) AS monetary
    FROM cleaned
    GROUP BY customer_id
)
SELECT
    customer_id,
    last_purchase_date,
    frequency,
    monetary,
    DATE_PART('day', (SELECT MAX(last_purchase_date) FROM rfm_base) - last_purchase_date) AS recency,
    NTILE(5) OVER (ORDER BY DATE_PART('day', (SELECT MAX(last_purchase_date) FROM rfm_base) - last_purchase_date) DESC) AS R_score,
    NTILE(5) OVER (ORDER BY frequency ASC) AS F_score,
    NTILE(5) OVER (ORDER BY monetary ASC) AS M_score
FROM rfm_base;