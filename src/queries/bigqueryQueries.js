// This file contains all the BigQuery queries used in the application.

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
    SAFE_DIVIDE(SUM(ad_spend), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS tacos
  FROM \`intentwise_ecommerce_graph.account_summary\`, date_ranges
  WHERE report_date BETWEEN date_ranges.start_date AND date_ranges.end_date
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
    SAFE_DIVIDE(SUM(ad_spend), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS tacos
  FROM \`intentwise_ecommerce_graph.account_summary\`, date_ranges
  WHERE report_date BETWEEN date_ranges.prev_start_date AND date_ranges.prev_end_date
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

  -- % Changes
  SAFE_DIVIDE((c.ad_spend - p.ad_spend), p.ad_spend) * 100 AS ad_spend_change_pct,
  SAFE_DIVIDE((c.ad_revenue - p.ad_revenue), p.ad_revenue) * 100 AS ad_revenue_change_pct,
  SAFE_DIVIDE((c.total_sales - p.total_sales), p.total_sales) * 100 AS total_sales_change_pct,
  SAFE_DIVIDE((c.organic_sales - p.organic_sales), p.organic_sales) * 100 AS organic_sales_change_pct,
  SAFE_DIVIDE((c.acos - p.acos), p.acos) * 100 AS acos_change_pct,
  SAFE_DIVIDE((c.roas - p.roas), p.roas) * 100 AS roas_change_pct,
  SAFE_DIVIDE((c.tacos - p.tacos), p.tacos) * 100 AS tacos_change_pct
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
  SUM(ad_revenue) AS ad_revenue,
  SUM(product_sales) + SUM(ordered_revenue) AS total_sales,
  CASE 
    WHEN (SUM(product_sales) + SUM(ordered_revenue)) - SUM(ad_revenue) > 0 
      THEN (SUM(product_sales) + SUM(ordered_revenue)) - SUM(ad_revenue)
    ELSE 0 
  END AS organic_sales
FROM 
  \`intentwise_ecommerce_graph.account_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
GROUP BY report_date ORDER BY report_date desc`;

const acosTacosTrend = `
SELECT 
  FORMAT_DATE('%Y-%m-%d', report_date) AS report_date,
  SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS acos,
  SAFE_DIVIDE(SUM(ad_spend), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS tacos
FROM 
  \`intentwise_ecommerce_graph.account_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
GROUP BY report_date
ORDER BY report_date;
`;

const totalUnitOrderedandSales = `
SELECT 
  FORMAT_DATE('%Y-%m-%d', report_date) AS report_date,
  SUM(product_quantity) AS units_ordered,
  SUM(product_sales) + SUM(ordered_revenue) AS total_sales
FROM 
  \`intentwise_ecommerce_graph.account_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
GROUP BY report_date
ORDER BY report_date;
`;

const productBySales = `
SELECT 
  product AS asin,
  sku,
  SAFE_ADD(SUM(product_sales), SUM(ordered_revenue)) AS total_sales,
  IFNULL(SUM(ad_revenue), 0) AS ad_revenue,
  IFNULL(SUM(ad_spend), 0) AS ad_spend
FROM 
  \`intentwise_ecommerce_graph.product_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
GROUP BY 
  asin, sku
ORDER BY 
  total_sales DESC
LIMIT @limit OFFSET @offset;
`;

const productSummary = `
SELECT 
  product_title,
  sku,
  product AS asin,

  IFNULL(SUM(ad_spend), 0) AS ad_spend,
  IFNULL(SUM(ad_revenue), 0) AS ad_revenue,

  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,

  IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0) AS total_sales,

  SAFE_DIVIDE(
    SUM(ad_spend),
    (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0))
  ) AS tacos,
  
  COUNT(*) OVER() AS total_count

FROM 
  \`intentwise_ecommerce_graph.product_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{where_clause}}

GROUP BY 
  product_title, sku, product
`;

const timeSeriesMetrics = `
    SELECT 'addRevenueTotalSalesTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${addRevenueTotalSalesTrend.replace(
		/;\s*$/,
		""
	)}) AS t
    UNION ALL
    SELECT 'acosTacosTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${acosTacosTrend.replace(
		/;\s*$/,
		""
	)}) AS t
    UNION ALL
    SELECT 'totalUnitOrderedandSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalUnitOrderedandSales.replace(
		/;\s*$/,
		""
	)}) AS t
    `;

const accountSummary = `
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices.replace(
		/;\s*$/,
		""
	)}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices.replace(
		/;\s*$/,
		""
	)}) AS t
    `;

module.exports = {
	accountSummaryMetrices: accountSummaryMetrices,
	addRevenueTotalSalesTrend: addRevenueTotalSalesTrend,
	selleCentralMetrices: selleCentralMetrices,
	acosTacosTrend: acosTacosTrend,
	productSummary: productSummary,
	acosTacosTrend: acosTacosTrend,
	totalUnitOrderedandSales: totalUnitOrderedandSales,
	productBySales: productBySales,
	timeSeriesMetrics: timeSeriesMetrics,
	accountSummary: accountSummary,
};
