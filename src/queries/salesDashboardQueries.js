// Sales Overview queries

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
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS FLOAT64) AS acos_percentage,
  CAST(IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0) AS FLOAT64) AS total_sales,


  CAST(SAFE_DIVIDE(
    SUM(ad_spend),
    (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(shipped_revenue), 0))
  ) AS FLOAT64) AS tacos_percentage,

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
  CAST(SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) * 100 AS FLOAT64) AS acos_percentage,

  -- ROAS = ad_revenue / ad_spend
  CAST(SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS FLOAT64) AS roas,

  -- TACOS % = (ad_spend / total_sales) * 100
  CAST(
    SAFE_DIVIDE(
      SUM(ad_spend),
      IFNULL(SUM(product_sales),0) + IFNULL(SUM(shipped_revenue),0)
    ) * 100 
    AS FLOAT64
  ) AS tacos_percentage

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

module.exports = {
	addRevenueTotalSalesTrend,
	acosTacosTrend,
	totalUnitOrderedandSales,
	productBySales,
	productSummary,
	totalSalesByWeek,
	totalSalesByMonth,
	weeklyTotalSales,
	monthlyTotalSales,
};
