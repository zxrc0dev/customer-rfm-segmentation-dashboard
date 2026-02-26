import pandas as pd
import re
from sqlalchemy import text
from sqlalchemy.engine import URL

def feature_engineering_sql(engine: URL, table_name: str, table_name_rfm: str):
    feature_engineering_query = f"""
    DROP TABLE IF EXISTS public.{table_name_rfm};

    CREATE TABLE public.{table_name_rfm} AS
    WITH
    /* 1) Base data (UNFILTERED except customer_id not null) */
    base AS (
        SELECT
            customer_id,
            invoice,
            CAST(invoice_date AS timestamp) AS invoice_date,
            quantity,
            price,
            (quantity * price) AS revenue
        FROM public.{table_name}
        WHERE customer_id IS NOT NULL
    ),

    /* 2) Global max invoice date */
    global_max AS (
        SELECT MAX(invoice_date) AS max_date
        FROM base
    ),

    /* 3) RFM core */
    rfm_core AS (
        SELECT
            b.customer_id,
            MAX(b.invoice_date) AS last_purchase_date,
            COUNT(DISTINCT b.invoice) AS frequency,
            SUM(b.revenue) AS monetary
        FROM base b
        GROUP BY b.customer_id
    ),

    rfm AS (
        SELECT
            r.customer_id,
            r.last_purchase_date,
            r.frequency,
            r.monetary,
            DATE_PART('day', (gm.max_date - r.last_purchase_date))::int AS recency
        FROM rfm_core r
        CROSS JOIN global_max gm
    ),

    /* 4) Scores (qcut approximation via NTILE) */
    scored AS (
        SELECT
            r.*,
            (6 - NTILE(5) OVER (ORDER BY r.recency ASC, r.customer_id)) AS recency_score,
            NTILE(5) OVER (ORDER BY r.frequency ASC, r.customer_id) AS frequency_score,
            NTILE(5) OVER (ORDER BY r.monetary ASC, r.customer_id) AS monetary_score
        FROM rfm r
    ),

    /* 5) Segment mapping from R_F_Score */
    segmented AS (
        SELECT
            s.*,
            (s.recency_score::text || s.frequency_score::text) AS r_f_score,
            CASE
                WHEN (s.recency_score::text || s.frequency_score::text) ~ '^[1-2][1-2]$' THEN 'hibernating'
                WHEN (s.recency_score::text || s.frequency_score::text) ~ '^[1-2][3-4]$' THEN 'at_risk'
                WHEN (s.recency_score::text || s.frequency_score::text) ~ '^[1-2]5$'     THEN 'cant_lose'
                WHEN (s.recency_score::text || s.frequency_score::text) ~ '^3[1-2]$'      THEN 'about_to_sleep'
                WHEN (s.recency_score::text || s.frequency_score::text) = '33'            THEN 'need_attention'
                WHEN (s.recency_score::text || s.frequency_score::text) ~ '^[3-4][4-5]$'  THEN 'loyal_customers'
                WHEN (s.recency_score::text || s.frequency_score::text) = '41'            THEN 'promising'
                WHEN (s.recency_score::text || s.frequency_score::text) = '51'            THEN 'new_customers'
                WHEN (s.recency_score::text || s.frequency_score::text) ~ '^[4-5][2-3]$'  THEN 'potential_loyalists'
                WHEN (s.recency_score::text || s.frequency_score::text) ~ '^5[4-5]$'      THEN 'champions'
                ELSE NULL
            END AS segment
        FROM scored s
    ),

    /* 6) Return ratio */
    sales_returns AS (
        SELECT
            customer_id,
            SUM(CASE WHEN revenue > 0 THEN revenue ELSE 0 END) AS total_sales,
            SUM(CASE WHEN revenue < 0 THEN -revenue ELSE 0 END) AS total_returns
        FROM base
        GROUP BY customer_id
    ),

    return_ratio AS (
        SELECT
            customer_id,
            COALESCE(total_returns / NULLIF(total_sales, 0), 0) AS return_ratio
        FROM sales_returns
    ),

    /* 7) Average interpurchase days */
    invoice_events AS (
        SELECT DISTINCT
            customer_id,
            invoice,
            invoice_date
        FROM base
    ),

    invoice_diffs AS (
        SELECT
            customer_id,
            EXTRACT(EPOCH FROM (invoice_date - LAG(invoice_date) OVER (
                PARTITION BY customer_id ORDER BY invoice_date
            ))) / 86400.0 AS delta_days
        FROM invoice_events
    ),

    avg_interpurchase AS (
        SELECT
            customer_id,
            AVG(delta_days) AS avg_interpurchase_days
        FROM invoice_diffs
        WHERE delta_days IS NOT NULL
        GROUP BY customer_id
    ),

    /* 8) Average order value */
    aov AS (
        SELECT
            customer_id,
            SUM(revenue) AS total_revenue,
            COUNT(DISTINCT invoice) AS total_orders,
            (SUM(revenue) / NULLIF(COUNT(DISTINCT invoice), 0)) AS avg_order_value
        FROM base
        GROUP BY customer_id
    )

    SELECT
        seg.customer_id,
        seg.last_purchase_date,
        seg.recency,
        seg.frequency,
        seg.monetary,
        seg.recency_score,
        seg.frequency_score,
        seg.monetary_score,
        seg.r_f_score,
        seg.segment,
        rr.return_ratio,
        ai.avg_interpurchase_days,
        a.avg_order_value
    FROM segmented seg
    LEFT JOIN return_ratio rr
        ON rr.customer_id = seg.customer_id
    LEFT JOIN avg_interpurchase ai
        ON ai.customer_id = seg.customer_id
    LEFT JOIN aov a
        ON a.customer_id = seg.customer_id
    ;
    """

    with engine.connect() as conn:
        conn.execute(text(feature_engineering_query))
        conn.commit()  # important for DDL in many SQLAlchemy setups
        print(f"Feature engineering table public.{table_name_rfm} created successfully.")
    

def feature_engineering_pandas(df: pd.DataFrame) -> pd.DataFrame:
    max_date = df['invoice_date'].max()

    df_rfm = df.groupby('customer_id').agg({
        'invoice_date': lambda x: (max_date - x.max()).days,
        'invoice': 'nunique',
        'revenue': 'sum'
    }).reset_index()

    df_rfm.rename(columns={
        'invoice_date': 'recency',
        'invoice': 'frequency',
        'revenue': 'monetary'
    }, inplace=True)

    df_rfm["recency_score"] = pd.qcut(
        df_rfm["recency"], 
        5, 
        labels=[5,4,3,2,1]
    )

    df_rfm["frequency_score"] = pd.qcut(
        df_rfm["frequency"].rank(method="first"), 
        5, 
        labels=[1,2,3,4,5]
    )

    df_rfm["monetary_score"] = pd.qcut(
        df_rfm["monetary"], 
        5, 
        labels=[1,2,3,4,5]
    )

    df_rfm["recency_score"] = df_rfm["recency_score"].astype(int)
    df_rfm["frequency_score"] = df_rfm["frequency_score"].astype(int)
    df_rfm["monetary_score"] = df_rfm["monetary_score"].astype(int)

    df_rfm["R_F_Score"] = df_rfm["recency_score"].astype(str) + df_rfm["frequency_score"].astype(str)

    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_risk',
        r'[1-2]5': 'cant_lose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }

    df_rfm["segment"] = df_rfm["R_F_Score"].replace(seg_map, regex = True)

    df['sales_only'] = df['revenue'].clip(lower=0)
    df['returns_only'] = df['revenue'].clip(upper=0).abs()

    ratios = df.groupby('customer_id').agg(
        total_sales=('sales_only', 'sum'),
        total_returns=('returns_only', 'sum')
    )

    ratios['return_ratio'] = ratios['total_returns'] / ratios['total_sales']
    ratios['return_ratio'] = ratios['return_ratio'].fillna(0)
    ratios.reset_index()
    ratios
    df_rfm = df_rfm.set_index('customer_id')
    df_rfm = df_rfm.merge(ratios[['return_ratio']], left_index=True, right_index=True, how='left')

    inv = (df.dropna(subset=["customer_id"])
            .drop_duplicates(subset=["customer_id", "invoice"])
            .sort_values(["customer_id", "invoice_date"]))

    avg_interpurchase = (inv.groupby("customer_id")["invoice_date"]
                        .diff()
                        .dt.total_seconds().div(86400)
                        .groupby(inv["customer_id"])
                        .mean()
                        .rename("avg_interpurchase_days"))
    
    df_rfm = df_rfm.merge(avg_interpurchase, left_index=True, right_index=True, how="left")
    """ 
    purchase_date = df.groupby("customer_id")["invoice_date"].min() 

    customer_tenure = (
        (max_date - purchase_date)
        .dt.total_seconds().div(86400)
        .rename("customer_tenure_days")
        .round()
        .astype("Int64")  
    )

    df_rfm = df_rfm.merge(customer_tenure, left_index=True, right_index=True, how="left")
    """
    avg_order_value = (
        df.groupby("customer_id")
        .agg(total_revenue=("revenue", "sum"),
            total_orders=("invoice", "nunique"))
    )

    avg_order_value["avg_order_value"] = avg_order_value["total_revenue"] / avg_order_value["total_orders"]

    df_rfm = df_rfm.merge(avg_order_value[["avg_order_value"]],
                        left_index=True, right_index=True, how="left")
    
    return df_rfm