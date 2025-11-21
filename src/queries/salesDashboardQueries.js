const accountSummaryMetrices = `
WITH
-- Calculate dynamic previous period based on date range
date_ranges AS (
  SELECT 
    DATE(@startDate) AS start_date,
    DATE(@endDate) AS end_date,
    DATE_SUB(DATE(@startDate), INTERVAL 1 DAY) AS prev_end_date,
    DATE_SUB(DATE(@startDate), INTERVAL DATE_DIFF(DATE(@endDate), DATE(@startDate), DAY) + 1 DAY) AS prev_start_date
),

-- Current period data
current_period AS (
  SELECT 
    IFNULL(SUM(ad_spend), 0) AS ad_spend,
    IFNULL(SUM(ad_revenue), 0) AS ad_revenue,
    IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0) AS total_sales,
    CASE 
      WHEN (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0)) - IFNULL(SUM(ad_revenue), 0) > 0 
        THEN (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0)) - IFNULL(SUM(ad_revenue), 0)
      ELSE 0 
    END AS organic_sales,
    SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS acos,
    SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
    IFNULL(AVG(product_quantity), 0) AS avg_daily_units,
    IFNULL(AVG(product_sales), 0) + IFNULL(AVG(ordered_revenue), 0) AS avg_daily_sales,
    SAFE_DIVIDE(SUM(ad_spend), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS tacos,
    IFNULL(SUM(ad_impressions), 0) AS impressions,
    IFNULL(SUM(ad_clicks), 0) AS clicks,
    SAFE_DIVIDE(SUM(ad_clicks), SUM(ad_impressions)) * 100 AS ctr,
    SAFE_DIVIDE(SUM(ad_conversions), SUM(ad_clicks)) * 100 AS cvr,
    SAFE_DIVIDE(SUM(ad_spend), SUM(ad_clicks)) AS avg_cpc,
    SAFE_DIVIDE(SUM(ad_revenue), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) * 100 AS ad_sales_attribute
  FROM `intentwise_ecommerce_graph.account_summary`, date_ranges
  WHERE report_date BETWEEN date_ranges.start_date AND date_ranges.end_date
  {{account_id_clause}}
),

-- Previous period data (auto calculated range)
previous_period AS (
  SELECT 
    IFNULL(SUM(ad_spend), 0) AS ad_spend,
    IFNULL(SUM(ad_revenue), 0) AS ad_revenue,
    IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0) AS total_sales,
    CASE 
      WHEN (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0)) - IFNULL(SUM(ad_revenue), 0) > 0 
        THEN (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0)) - IFNULL(SUM(ad_revenue), 0)
      ELSE 0 
    END AS organic_sales,
    SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS acos,
    SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
    IFNULL(AVG(product_quantity), 0) AS avg_daily_units,
    IFNULL(AVG(product_sales), 0) + IFNULL(AVG(ordered_revenue), 0) AS avg_daily_sales,
    SAFE_DIVIDE(SUM(ad_spend), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS tacos,
    IFNULL(SUM(ad_impressions), 0) AS impressions,
    IFNULL(SUM(ad_clicks), 0) AS clicks,
    SAFE_DIVIDE(SUM(ad_clicks), SUM(ad_impressions)) * 100 AS ctr,
    SAFE_DIVIDE(SUM(ad_conversions), SUM(ad_clicks)) * 100 AS cvr,
    SAFE_DIVIDE(SUM(ad_spend), SUM(ad_clicks)) AS avg_cpc,
    SAFE_DIVIDE(SUM(ad_revenue), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) * 100 AS ad_sales_attribute
  FROM `intentwise_ecommerce_graph.account_summary`, date_ranges
  WHERE report_date BETWEEN date_ranges.prev_start_date AND date_ranges.prev_end_date
  {{account_id_clause}}
)

-- Final output with percentage changes
SELECT
  -- Current period values
  c.ad_spend,
  c.ad_revenue,
  c.total_sales,
  c.organic_sales,
  c.acos,
  c.roas,
  c.tacos,
  c.avg_daily_units,
  c.avg_daily_sales,
  c.impressions,
  c.clicks,
  c.ctr,
  c.cvr,
  c.avg_cpc,
  c.ad_sales_attribute,

  -- % Changes
  SAFE_DIVIDE((c.ad_spend - p.ad_spend), p.ad_spend) * 100 AS ad_spend_change_pct,
  SAFE_DIVIDE((c.ad_revenue - p.ad_revenue), p.ad_revenue) * 100 AS ad_revenue_change_pct,
  SAFE_DIVIDE((c.total_sales - p.total_sales), p.total_sales) * 100 AS total_sales_change_pct,
  SAFE_DIVIDE((c.organic_sales - p.organic_sales), p.organic_sales) * 100 AS organic_sales_change_pct,
  SAFE_DIVIDE((c.acos - p.acos), p.acos) * 100 AS acos_change_pct,
  SAFE_DIVIDE((c.roas - p.roas), p.roas) * 100 AS roas_change_pct,
  SAFE_DIVIDE((c.avg_daily_units - p.avg_daily_units), p.avg_daily_units) * 100 AS avg_daily_units_change_pct,
  SAFE_DIVIDE((c.avg_daily_sales - p.avg_daily_sales), p.avg_daily_sales) * 100 AS avg_daily_sales_change_pct,
  SAFE_DIVIDE((c.tacos - p.tacos), p.tacos) * 100 AS tacos_change_pct,
  SAFE_DIVIDE((c.impressions - p.impressions), p.impressions) * 100 AS impressions_change_pct,
  SAFE_DIVIDE((c.clicks - p.clicks), p.clicks) * 100 AS clicks_change_pct,
  SAFE_DIVIDE((c.ctr - p.ctr), p.ctr) * 100 AS ctr_change_pct,
  SAFE_DIVIDE((c.cvr - p.cvr), p.cvr) * 100 AS cvr_change_pct,
  SAFE_DIVIDE((c.avg_cpc - p.avg_cpc), p.avg_cpc) * 100 AS avg_cpc_change_pct
FROM current_period c, previous_period p;
`;
} 