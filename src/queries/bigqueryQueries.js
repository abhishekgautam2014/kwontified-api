// This file contains all the BigQuery queries used in the application.
// Sales Overview queries
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
  FROM \`intentwise_ecommerce_graph.account_summary\`, date_ranges
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
  FROM \`intentwise_ecommerce_graph.account_summary\`, date_ranges
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

const selleCentralMetrices = `
WITH
-- Step 1: Define current and previous date ranges dynamically
date_ranges AS (
  SELECT 
    DATE(@startDate) AS start_date,
    DATE(@endDate) AS end_date,
    DATE_SUB(DATE(@startDate), INTERVAL 1 DAY) AS prev_end_date,
    DATE_SUB(DATE(@startDate), INTERVAL DATE_DIFF(DATE(@endDate), DATE(@startDate), DAY) + 1 DAY) AS prev_start_date
),

-- Step 2: Current period metrics
current_period AS (
  SELECT 
    IFNULL(SUM(units_ordered), 0) AS units_ordered,
    IFNULL(SUM(total_order_items), 0) AS total_order_items,
    IFNULL(AVG(avg_sales_per_order_item_amt), 0) AS avg_sales_per_order_item_amt,
    IFNULL(AVG(avg_selling_price_amt), 0) AS avg_selling_price_amt,
    IFNULL(SUM(sessions), 0) AS sessions,
    IFNULL(SUM(page_views), 0) AS page_views,
    IFNULL(AVG(avg_offer_count), 0) AS avg_offer_count,
    IFNULL(AVG(refund_rate), 0) AS refund_rate
  FROM \`amazon_source_data.sellercentral_salesandtrafficbydate_report\`, date_ranges
  WHERE sale_date BETWEEN date_ranges.start_date AND date_ranges.end_date
  {{account_id_clause}}
),

-- Step 3: Previous period metrics
previous_period AS (
  SELECT 
    IFNULL(SUM(units_ordered), 0) AS units_ordered,
    IFNULL(SUM(total_order_items), 0) AS total_order_items,
    IFNULL(AVG(avg_sales_per_order_item_amt), 0) AS avg_sales_per_order_item_amt,
    IFNULL(AVG(avg_selling_price_amt), 0) AS avg_selling_price_amt,
    IFNULL(SUM(sessions), 0) AS sessions,
    IFNULL(SUM(page_views), 0) AS page_views,
    IFNULL(AVG(avg_offer_count), 0) AS avg_offer_count,
    IFNULL(AVG(refund_rate), 0) AS refund_rate
  FROM \`amazon_source_data.sellercentral_salesandtrafficbydate_report\`, date_ranges
  WHERE sale_date BETWEEN date_ranges.prev_start_date AND date_ranges.prev_end_date
  {{account_id_clause}}
)

-- Step 4: Combine current & previous metrics + calculate percentage changes
SELECT
  -- Current metrics
  c.units_ordered,
  c.total_order_items,
  c.avg_sales_per_order_item_amt,
  c.avg_selling_price_amt,
  c.sessions,
  c.page_views,
  c.avg_offer_count,
  c.refund_rate,

  -- % change vs previous period
  SAFE_DIVIDE((c.units_ordered - p.units_ordered), p.units_ordered) * 100 AS units_ordered_change_pct,
  SAFE_DIVIDE((c.total_order_items - p.total_order_items), p.total_order_items) * 100 AS total_order_items_change_pct,
  SAFE_DIVIDE((c.avg_sales_per_order_item_amt - p.avg_sales_per_order_item_amt), p.avg_sales_per_order_item_amt) * 100 AS avg_sales_per_order_item_amt_change_pct,
  SAFE_DIVIDE((c.avg_selling_price_amt - p.avg_selling_price_amt), p.avg_selling_price_amt) * 100 AS avg_selling_price_amt_change_pct,
  SAFE_DIVIDE((c.sessions - p.sessions), p.sessions) * 100 AS sessions_change_pct,
  SAFE_DIVIDE((c.page_views - p.page_views), p.page_views) * 100 AS page_views_change_pct,
  SAFE_DIVIDE((c.avg_offer_count - p.avg_offer_count), p.avg_offer_count) * 100 AS avg_offer_count_change_pct,
  SAFE_DIVIDE((c.refund_rate - p.refund_rate), p.refund_rate) * 100 AS refund_rate_change_pct

FROM current_period c, previous_period p;
`;

const addRevenueTotalSalesTrend = `
SELECT 
  FORMAT_DATE('%Y-%m-%d', report_date) AS report_date,
  CAST(SUM(ad_revenue) AS FLOAT64) AS ad_revenue,
  CAST(SUM(ad_spend) AS FLOAT64) AS ad_spend,
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS FLOAT64) AS roas,
  CAST(SUM(product_sales) + SUM(ordered_revenue) AS FLOAT64) AS total_sales,
  CASE 
    WHEN (SUM(product_sales) + SUM(ordered_revenue)) - SUM(ad_revenue) > 0 
      THEN CAST(SUM(product_sales) + SUM(ordered_revenue) AS FLOAT64) - CAST(SUM(ad_revenue) AS FLOAT64)
    ELSE 0 
  END AS organic_sales
FROM 
  \`intentwise_ecommerce_graph.account_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{account_id_clause}}
GROUP BY report_date ORDER BY report_date`;

const acosTacosTrend = `
SELECT
  FORMAT_DATE('%Y-%m-%d', report_date) AS report_date,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS FLOAT64) AS acos,
  CAST(SAFE_DIVIDE(SUM(ad_spend), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS FLOAT64) AS tacos
FROM 
  \`intentwise_ecommerce_graph.account_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{account_id_clause}}
GROUP BY report_date
ORDER BY report_date;
`;

const totalUnitOrderedandSales = `
SELECT
  FORMAT_DATE('%Y-%m-%d', report_date) AS report_date,
  SUM(product_quantity) AS units_ordered,
  CAST(SUM(product_sales) + SUM(ordered_revenue) AS FLOAT64) AS total_sales
FROM 
  \`intentwise_ecommerce_graph.account_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{account_id_clause}}
GROUP BY report_date
ORDER BY report_date;
`;

const productBySales = `
SELECT 
  product AS asin,
  sku,
  CAST(IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0) AS FLOAT64) AS total_sales,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend
FROM 
  \`intentwise_ecommerce_graph.product_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id {{where_clause}}
GROUP BY 
  product, sku
ORDER BY 
  total_sales DESC
`;

const productSummary = `
SELECT 
  product_title,
  sku,
  product AS asin,

  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,

  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS FLOAT64) AS roas,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS FLOAT64) AS acos,
  CAST(IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0) AS FLOAT64) AS total_sales,


  CAST(SAFE_DIVIDE(
    SUM(ad_spend),
    (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0))
  ) AS FLOAT64) AS tacos,

  COUNT(*) OVER() AS total_count

FROM 
  \`intentwise_ecommerce_graph.product_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{where_clause}}
GROUP BY 
  product_title, sku, product
ORDER BY 
  total_sales DESC
`;

const totalSalesByWeek = `
WITH date_ranges AS (
  SELECT
    DATE(@startDate) AS start_date,
    DATE(@endDate) AS end_date,
    DATE_SUB(DATE(@startDate), INTERVAL 1 YEAR) AS prev_start_date,
    DATE_SUB(DATE(@endDate), INTERVAL 1 YEAR) AS prev_end_date
),

-- Current period weekly total sales
current_period AS (
  SELECT 
    EXTRACT(ISOWEEK FROM report_date) AS week_number,
    EXTRACT(YEAR FROM report_date) AS year_number,
    DATE_TRUNC(report_date, WEEK(MONDAY)) AS week_start,
    SUM(IFNULL(product_sales,0) + IFNULL(ordered_revenue,0)) AS total_sales
  FROM \`intentwise_ecommerce_graph.product_summary\`, date_ranges
  WHERE report_date BETWEEN date_ranges.start_date AND date_ranges.end_date
    /* {{account_id_clause}} */
  GROUP BY week_number, year_number, week_start
),

-- Same week last year: weekly total sales
previous_period AS (
  SELECT 
    EXTRACT(ISOWEEK FROM report_date) AS week_number,
    EXTRACT(YEAR FROM report_date) AS year_number,
    DATE_TRUNC(report_date, WEEK(MONDAY)) AS week_start,
    SUM(IFNULL(product_sales,0) + IFNULL(ordered_revenue,0)) AS total_sales_last_year
  FROM \`intentwise_ecommerce_graph.product_summary\`, date_ranges
  WHERE report_date BETWEEN date_ranges.prev_start_date AND date_ranges.prev_end_date
    /* {{account_id_clause}} */
  GROUP BY week_number, year_number, week_start
)

-- Final Output
SELECT
  -- Current week start date
  FORMAT_DATE('%Y-%m-%d', c.week_start) AS week_start,

  -- Current week number
  c.week_number AS current_week_number,

  -- Last year week start date
  FORMAT_DATE('%Y-%m-%d', p.week_start) AS week_start_last_year,

  -- Last year week number
  p.week_number AS last_year_week_number,

  -- Sales
  c.total_sales AS current_total_sales,
  p.total_sales_last_year AS last_year_total_sales,

  -- YoY growth
  SAFE_DIVIDE(c.total_sales - p.total_sales_last_year, p.total_sales_last_year) AS yoy_growth

FROM current_period c
LEFT JOIN previous_period p
  ON c.week_number = p.week_number
 AND c.year_number - 1 = p.year_number
ORDER BY c.week_start;
`;

const totalSalesByMonth = `
SELECT
  -- Month name + year (e.g., June 25)
  FORMAT_DATE('%B %y', DATE_TRUNC(report_date, MONTH)) AS month_label,

  -- Month start and end dates
  DATE_TRUNC(report_date, MONTH) AS month_start,
  DATE_SUB(
    DATE_ADD(DATE_TRUNC(report_date, MONTH), INTERVAL 1 MONTH),
    INTERVAL 1 DAY
  ) AS month_end,

  -- Aggregated sales metrics
  CAST(IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0) AS FLOAT64) AS total_sales,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend,

  -- Performance metrics
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) * 100 AS FLOAT64) AS acos_percent,
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS FLOAT64) AS roas,
  CAST(
    SAFE_DIVIDE(
      SUM(ad_spend),
      (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0))
    ) * 100
    AS FLOAT64
  ) AS tacos_percent

FROM 
  \`intentwise_ecommerce_graph.product_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  {{where_clause}}

GROUP BY 
  month_label, month_start, month_end

ORDER BY 
  month_start ASC;
`;

const weeklyTotalSales = `
WITH weekly_data AS (
  SELECT
    DATE_TRUNC(report_date, WEEK(MONDAY)) AS week_start,
    DATE_ADD(DATE_TRUNC(report_date, WEEK(MONDAY)), INTERVAL 6 DAY) AS week_end,
    EXTRACT(ISOWEEK FROM report_date) AS week_number,
    EXTRACT(YEAR FROM report_date) AS year_number,

    IFNULL(product_sales, 0) AS product_sales,
    IFNULL(shipped_revenue, 0) AS shipped_revenue,
    IFNULL(ad_revenue, 0) AS ad_revenue,
    IFNULL(ad_spend, 0) AS ad_spend
  FROM 
    \`intentwise_ecommerce_graph.product_summary\`
  WHERE 
    report_date BETWEEN @startDate AND @endDate
    AND account_id = @account_id
    {{where_clause}}
)

SELECT
  FORMAT_DATE('%Y-%m-%d', week_start) AS week_start_date,
  FORMAT_DATE('%Y-%m-%d', week_end) AS week_end_date,
  week_number,
  year_number,

  -- Aggregated totals
  CAST(SUM(product_sales + shipped_revenue) AS FLOAT64) AS total_sales,
  CAST(SUM(ad_revenue) AS FLOAT64) AS ad_revenue,
  CAST(SUM(ad_spend) AS FLOAT64) AS ad_spend,

  -- ACOS % = (ad_spend / ad_revenue) * 100
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) * 100 AS FLOAT64) AS acos,

  -- ROAS = ad_revenue / ad_spend
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS FLOAT64) AS roas,

  -- TACOS % = (ad_spend / total_sales) * 100
  CAST(
    SAFE_DIVIDE(
      SUM(ad_spend),
      IFNULL(SUM(product_sales),0) + IFNULL(SUM(shipped_revenue),0)
    ) * 100 
    AS FLOAT64
  ) AS tacos

FROM weekly_data
GROUP BY 
  week_start, week_end, week_number, year_number
ORDER BY 
  week_start DESC;
`;

const monthlyTotalSales = `
SELECT
  -- Month name with year (e.g., June 25)
  FORMAT_DATE('%B %y', DATE_TRUNC(report_date, MONTH)) AS month_year,

  -- Month start and end
  DATE_TRUNC(report_date, MONTH) AS month_start,
  DATE_SUB(
    DATE_ADD(DATE_TRUNC(report_date, MONTH), INTERVAL 1 MONTH),
    INTERVAL 1 DAY
  ) AS month_end,

  -- Aggregated sales metrics
  CAST(IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0) AS FLOAT64) AS total_sales,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend,

  -- Performance metrics
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) * 100 AS FLOAT64) AS acos_percent,
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS FLOAT64) AS roas,
  CAST(
    SAFE_DIVIDE(
      SUM(ad_spend),
      (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0))
    ) * 100
    AS FLOAT64
  ) AS tacos_percent

FROM 
  \`intentwise_ecommerce_graph.product_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  {{where_clause}}

GROUP BY 
  month_year, month_start, month_end

ORDER BY 
  month_start ASC;
`;

const getTimeSeriesMetricsQuery = (accountIdClause) => `
    SELECT 'addRevenueTotalSalesTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${addRevenueTotalSalesTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'acosTacosTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${acosTacosTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalUnitOrderedandSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalUnitOrderedandSales
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

const getAccountSummaryQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

const getDashboardMetricsQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'addRevenueTotalSalesTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${addRevenueTotalSalesTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'acosTacosTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${acosTacosTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalUnitOrderedandSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalUnitOrderedandSales
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
	UNION ALL
	SELECT 'productBySales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${productBySales
		.replace("{{where_clause}}", "")
		.replace(/;\s*$/, "")}) AS t
`;

const getYearlyMonthSalesDashboardMetricsQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'addRevenueTotalSalesTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${addRevenueTotalSalesTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'acosTacosTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${acosTacosTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalUnitOrderedandSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalUnitOrderedandSales
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
	UNION ALL
	SELECT 'productBySales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${productBySales
		.replace("{{where_clause}}", "")
		.replace(/;\s*$/, "")}) AS t
`;

//-------------- campaign summary queries (Advertising Menu)-------------------------//
const impressionsClicksTrend = `
SELECT 
  FORMAT_DATE('%Y-%m-%d', report_date) AS report_date,
  IFNULL(SUM(ad_impressions), 0) AS ad_impressions,
  IFNULL(SUM(ad_clicks), 0) AS ad_clicks
FROM 
  \`intentwise_ecommerce_graph.campaign_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  /* {{account_id_clause}} */
GROUP BY report_date
ORDER BY report_date;
`;

const cpcwithPrevMonthCpcTrend = `
WITH
date_ranges AS (
  SELECT
    DATE(@startDate) AS start_date,
    DATE(@endDate) AS end_date,
    DATE_SUB(DATE(@startDate), INTERVAL 1 MONTH) AS prev_start_date,
    DATE_SUB(DATE(@endDate), INTERVAL 1 MONTH) AS prev_end_date
),

-- Current period daily CPC
current_period AS (
  SELECT 
    report_date,
    CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_clicks)) AS FLOAT64) AS cpc
  FROM \`intentwise_ecommerce_graph.account_summary\`, date_ranges
  WHERE report_date BETWEEN date_ranges.start_date AND date_ranges.end_date
    /* {{account_id_clause}} */
  GROUP BY report_date
),

-- Previous period daily CPC (same date offset last month)
previous_period AS (
  SELECT 
    report_date,
    CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_clicks)) AS FLOAT64) AS prev_cpc
  FROM \`intentwise_ecommerce_graph.account_summary\`, date_ranges
  WHERE report_date BETWEEN date_ranges.prev_start_date AND date_ranges.prev_end_date
    /* {{account_id_clause}} */
  GROUP BY report_date
)

-- Final output: join by day offset
SELECT
  FORMAT_DATE('%Y-%m-%d', c.report_date) AS report_date,
  c.cpc AS current_cpc,
  p.prev_cpc AS previous_cpc
FROM current_period c
LEFT JOIN previous_period p
  ON DATE_SUB(c.report_date, INTERVAL 1 MONTH) = p.report_date
ORDER BY c.report_date;
`;

const capmpaignPerformanceTable = `
SELECT 
  CASE 
    WHEN campaign_type = 'SP M PT' THEN 'Sponsored Products - Product Targeting'
    WHEN campaign_type = 'SP M KT' THEN 'Sponsored Products - Keyword Targeting'
    WHEN campaign_type = 'SB M PT' THEN 'Sponsored Brands - Product Targeting'
    WHEN campaign_type = 'SP A' THEN 'Sponsored Products - Auto Targeting'
    WHEN campaign_type = 'SB M KT' THEN 'Sponsored Brands - Keyword Targeting'
    WHEN campaign_type = 'SB M V' THEN 'Sponsored Brands - Video'
    WHEN campaign_type = 'SD PT' THEN 'Sponsored Display - Product Targeting'
    WHEN campaign_type = 'SD A' THEN 'Sponsored Display - Audiences'
    WHEN campaign_type = 'SB M V PT' THEN 'Sponsored Brands - Video - Product Targeting'
    WHEN campaign_type = 'SP M KT, SP M PT' THEN 'Sponsored Products - Keyword/Product Targeting'
    ELSE 'Other'
  END AS campaign_type,

  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS FLOAT64) AS acos,
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS FLOAT64) AS roas,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(SUM(ad_spend)) OVER ()) AS FLOAT64) AS ad_spend_percentage,
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(SUM(ad_revenue)) OVER ()) AS FLOAT64) AS ad_revenue_percentage

FROM 
  \`intentwise_ecommerce_graph.campaign_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{where_clause}}
GROUP BY 
  campaign_type
ORDER BY 
  ad_revenue DESC;

`;

const capmpaignSummaryTable = `
SELECT 
  campaign_name,
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS FLOAT64) AS acos,
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS FLOAT64) AS roas,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(SUM(ad_spend)) OVER ()) AS FLOAT64) AS ad_spend_percentage,
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(SUM(ad_revenue)) OVER ()) AS FLOAT64) AS ad_revenue_percentage,
  CAST(IFNULL(SUM(ad_impressions), 0) AS FLOAT64) AS ad_impressions,
  CAST(IFNULL(SUM(ad_clicks), 0) AS FLOAT64) AS ad_clicks,
  CAST(SAFE_DIVIDE(SUM(ad_clicks), SUM(ad_impressions)) * 100 AS FLOAT64) AS ctr,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_clicks)) AS FLOAT64) AS avg_cpc
FROM 
  \`intentwise_ecommerce_graph.campaign_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{where_clause}}
GROUP BY 
  campaign_name
ORDER BY ad_revenue desc
`;

const sponsoredProductPerformanceTable = `
SELECT 
  product AS asin,
  product_title,
  sku,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS FLOAT64) AS acos,
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS FLOAT64) AS roas,
  IFNULL(SUM(ad_impressions), 0) AS ad_impressions,
  IFNULL(SUM(ad_clicks), 0) AS ad_clicks
FROM 
  \`intentwise_ecommerce_graph.product_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{where_clause}}
GROUP BY 
  asin, sku, product_title
`;

const getAdvertisingDashboardQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'selleCentralMetrices' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'impressionsClicksTrend' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${impressionsClicksTrend
		.replace("/* {{account_id_clause}} */", accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'cpcwithPrevMonthCpcTrend' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${cpcwithPrevMonthCpcTrend.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'capmpaignPerformanceTable' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${capmpaignPerformanceTable
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'capmpaignSummaryTable' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${capmpaignSummaryTable
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'sponsoredProductPerformanceTable' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${sponsoredProductPerformanceTable
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'addRevenueTotalSalesTrend' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${addRevenueTotalSalesTrend
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

// -----------------------------------Order Dashboard queries ---------------------------------- //
const orderFulfillment = `
SELECT 
  fulfillment_channel, count(fulfillment_channel) AS total_orders
FROM \`amazon_source_data.sellercentral_ordersbydate_report\`
WHERE 
  purchase_date BETWEEN @startDate AND @endDate
GROUP BY 
  fulfillment_channel
`;

const orderedProductPerformance = `
SELECT 
  parent_asin, sku, child_asin,
  IFNULL(SUM(ordered_product_sales_amt), 0) AS total_sales,
  IFNULL(SUM(total_order_items), 0) AS total_order_items,
  IFNULL(SUM(units_ordered), 0) AS units_ordered,
  SAFE_DIVIDE(SUM(ordered_product_sales_amt), SUM(SUM(ordered_product_sales_amt)) OVER ()) AS total_sales_percentage,
  SAFE_DIVIDE(SUM(total_order_items), SUM(SUM(total_order_items)) OVER ()) AS total_order_items_percentage,
  SAFE_DIVIDE(SUM(units_ordered), SUM(SUM(units_ordered)) OVER ()) AS units_ordered_percentage
FROM \`amazon_source_data.sellercentral_salesandtrafficbysku_report\`
WHERE 
  sale_date BETWEEN @startDate AND @endDate
  {{where_clause}}
GROUP BY 
  parent_asin, sku, child_asin
`;

const totalUnitsOrderSales = `
SELECT 
  FORMAT_DATE('%Y-%m-%d', sale_date) AS report_date,
  IFNULL(SUM(ordered_product_sales_amt), 0) AS total_sales,
  IFNULL(SUM(total_order_items), 0) AS total_order_items,
  IFNULL(SUM(units_ordered), 0) AS units_ordered
FROM \`amazon_source_data.sellercentral_salesandtrafficbydate_report\`
WHERE 
  sale_date BETWEEN @startDate AND @endDate
GROUP BY 
  sale_date
`;

const getOrderDashboardQuery = (accountIdClause) => `
    SELECT 'orderFulfillment' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${orderFulfillment
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'orderedProductPerformance' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${orderedProductPerformance
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalUnitsOrderSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalUnitsOrderSales
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

const get12monthSalesQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalSalesByWeek' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalSalesByWeek
		.replace("/* {{account_id_clause}} */", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalSalesByMonth' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalSalesByMonth
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'productSummary' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${productSummary
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'weeklyTotalSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${weeklyTotalSales
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'monthlyTotalSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${monthlyTotalSales
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

//Advertising Targetting Analysis Queries
const spendByBrand = `
SELECT 
  CASE 
    WHEN is_brand_term = TRUE THEN "Brand" 
    ELSE "Non Brand" 
  END AS brand_type,
  
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend

FROM 
  \`intentwise_ecommerce_graph.searchterm_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  {{where_clause}}

GROUP BY 
  brand_type
ORDER BY 
  brand_type;
`;

const revenueByBrand = `
SELECT 
  CASE 
    WHEN is_brand_term = TRUE THEN "Brand" 
    ELSE "Non Brand" 
  END AS brand_type,
  
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue

FROM 
  \`intentwise_ecommerce_graph.searchterm_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  {{where_clause}}

GROUP BY 
  brand_type
ORDER BY 
  brand_type;
`;
//     SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
const roasByBrand = `
SELECT 
  CASE 
    WHEN is_brand_term = TRUE THEN "Brand" 
    ELSE "Non Brand" 
  END AS brand_type,
  
  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas
FROM 
  \`intentwise_ecommerce_graph.searchterm_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  {{where_clause}}

GROUP BY 
  brand_type
ORDER BY 
  brand_type;
`;

const brandOverview = `
SELECT 
  CASE 
    WHEN is_brand_term = TRUE THEN "Brand" 
    ELSE "Non Brand" 
  END AS brand_type,
  
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,
  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) * 100 AS FLOAT64) AS acos,
FROM 
  \`intentwise_ecommerce_graph.searchterm_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  {{where_clause}}

GROUP BY 
  brand_type
ORDER BY 
  brand_type;
`;

const spendByMatchType = `
SELECT 
  match_type,
  
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend

FROM 
  \`intentwise_ecommerce_graph.searchterm_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  AND match_type IS NOT NULL
  AND match_type != ''
  {{where_clause}}

GROUP BY 
  match_type
ORDER BY 
  match_type;
`;

const revenueByMatchType = `
SELECT 
  match_type,
  
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue

FROM 
  \`intentwise_ecommerce_graph.searchterm_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  AND match_type IS NOT NULL
  AND match_type != ''
  {{where_clause}}

GROUP BY 
  match_type
ORDER BY 
  match_type;
`;
//     SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
const roasByMatchType = `
SELECT 
  match_type,
  
  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas
FROM 
  \`intentwise_ecommerce_graph.searchterm_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  AND match_type IS NOT NULL
  AND match_type != ''
  {{where_clause}}

GROUP BY 
  match_type
ORDER BY 
  match_type;
`;

const matchTypeOverview = `
SELECT 
  match_type,
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,
  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) * 100 AS FLOAT64) AS acos
FROM 
  \`intentwise_ecommerce_graph.searchterm_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  AND match_type IS NOT NULL
  AND match_type != ''
  {{where_clause}}
GROUP BY 
  match_type
ORDER BY 
  match_type;
`;

const keywordSummary = `
SELECT 
  match_type,
  keyword_text,
  CAST(IFNULL(SUM(ad_spend), 0) AS FLOAT64) AS ad_spend,
  CAST(IFNULL(SUM(ad_revenue), 0) AS FLOAT64) AS ad_revenue,
  SAFE_DIVIDE(SUM(ad_conversions), SUM(ad_clicks)) AS cvr,
  SAFE_DIVIDE(SUM(ad_spend), SUM(ad_clicks)) AS avg_cpc,
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) * 100 AS FLOAT64) AS acos
FROM 
  \`intentwise_ecommerce_graph.keyword_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
  AND match_type IS NOT NULL
  AND match_type != ''
  {{where_clause}}
GROUP BY 
  match_type,
  keyword_text
ORDER BY 
  ad_revenue DESC;
`;

const getAdvertisingTargetingAnalysisQuery = (accountIdClause) => `
    SELECT 'spendByBrand' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${spendByBrand
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'revenueByBrand' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${revenueByBrand
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'roasByBrand' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${roasByBrand
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'brandOverview' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${brandOverview
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'spendByMatchType' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${spendByMatchType
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'revenueByMatchType' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${revenueByMatchType
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'roasByMatchType' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${roasByMatchType
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'matchTypeOverview' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${matchTypeOverview
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'keywordSummary' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${keywordSummary
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

module.exports = {
	accountSummaryMetrices,
	addRevenueTotalSalesTrend,
	selleCentralMetrices,
	acosTacosTrend,
	productSummary,
	totalUnitOrderedandSales,
	productBySales,
	totalSalesByWeek,
	totalSalesByMonth,
	weeklyTotalSales,
	monthlyTotalSales,
	cpcwithPrevMonthCpcTrend,
	impressionsClicksTrend,
	capmpaignPerformanceTable,
	capmpaignSummaryTable,
	sponsoredProductPerformanceTable,
	orderedProductPerformance,
	orderFulfillment,
	totalUnitsOrderSales,
	timeSeriesMetrics: getTimeSeriesMetricsQuery,
	accountSummary: getAccountSummaryQuery,
	dashboardMetrics: getDashboardMetricsQuery,
	yearlyMonthSalesDashboardMetrics: getYearlyMonthSalesDashboardMetricsQuery,
	advertisingDashboard: getAdvertisingDashboardQuery,
	orderDashboard: getOrderDashboardQuery,
	"12monthSales": get12monthSalesQuery,
	advertisingTargetingAnalysis: getAdvertisingTargetingAnalysisQuery,
	spendByBrand,
	revenueByBrand,
	roasByBrand,
	brandOverview,
	spendByMatchType,
	revenueByMatchType,
	roasByMatchType,
	matchTypeOverview,
	keywordSummary,
};
