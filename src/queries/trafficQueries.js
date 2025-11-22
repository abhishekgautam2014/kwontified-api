const buyBoxPercentageTrendQuery = `
WITH
date_ranges AS (
  SELECT
    DATE(@startDate) AS start_date,
    DATE(@endDate) AS end_date,
    DATE_SUB(DATE(@startDate), INTERVAL 1 MONTH) AS prev_start_date,
    DATE_SUB(DATE(@endDate), INTERVAL 1 MONTH) AS prev_end_date
),

-- Current period daily buy_box_percentage
current_period AS (
  SELECT 
    sale_date,
    CONCAT(ROUND(AVG(buy_box_percentage), 2), '%') AS buy_box_percentage,   
    CAST(IFNULL(SUM(avg_offer_count), 0) AS FLOAT64) AS avg_offer_count
  FROM \`amazon_source_data.sellercentral_salesandtrafficbydate_report\`, date_ranges
  WHERE sale_date BETWEEN date_ranges.start_date AND date_ranges.end_date
    /* {{account_id_clause}} */
  GROUP BY sale_date
),

-- Previous period daily buy_box_percentage (same date offset last month)
previous_period AS (
  SELECT 
    sale_date,
    CONCAT(ROUND(AVG(buy_box_percentage), 2), '%') AS prev_buy_box_percentage,
    CAST(IFNULL(SUM(avg_offer_count), 0) AS FLOAT64) AS prev_avg_offer_count
  FROM \`amazon_source_data.sellercentral_salesandtrafficbydate_report\`, date_ranges
  WHERE sale_date BETWEEN date_ranges.prev_start_date AND date_ranges.prev_end_date
    /* {{account_id_clause}} */
  GROUP BY sale_date
)

-- Final output: join by day offset
SELECT
    FORMAT_DATE('%Y-%m-%d', c.sale_date) AS sale_date,
    c.buy_box_percentage AS current_buy_box_percentage,
    p.prev_buy_box_percentage AS previous_buy_box_percentage,
    c.avg_offer_count AS current_avg_offer_count,
    p.prev_avg_offer_count AS previous_avg_offer_count
FROM current_period c
LEFT JOIN previous_period p
  ON DATE_SUB(c.sale_date, INTERVAL 1 MONTH) = p.sale_date
ORDER BY c.sale_date;
`;

const trafficProductPerformanceTableQuery = `
SELECT 
  parent_asin, child_asin,
  CAST(IFNULL(SUM(ordered_product_sales_amt), 0) AS FLOAT64) AS total_sales,
  CAST(IFNULL(SUM(units_ordered), 0) AS FLOAT64) AS units_ordered,
  CAST(IFNULL(SUM(traffic_by_asin_sessions), 0) AS FLOAT64) AS sessions,
  CONCAT(ROUND(SUM(traffic_by_asin_sessions), 2), '%') AS sessions_percentage,
  CAST(IFNULL(SUM(traffic_by_asin_page_views), 0) AS FLOAT64) AS page_views,
  CONCAT(ROUND(SUM(traffic_by_asin_page_views), 2), '%') AS page_views_percentage,
  CONCAT(ROUND(AVG(traffic_by_asin_buy_box_prcntg), 2), '%') AS buy_box_percentage
FROM 
  \`amazon_source_data.sellercentral_salesandtrafficbychildasin_report\`
WHERE 
  sale_date BETWEEN @startDate AND @endDate
  {{where_clause}}
GROUP BY 
  parent_asin, child_asin
`;

const trafficSessionsPageviewsQuery = `
SELECT 
  FORMAT_DATE('%Y-%m-%d', sale_date) AS sale_date,
  CAST(IFNULL(SUM(traffic_by_asin_sessions), 0) AS FLOAT64) AS sessions,
  CAST(IFNULL(SUM(traffic_by_asin_page_views), 0) AS FLOAT64) AS page_views
FROM 
  \`amazon_source_data.sellercentral_salesandtrafficbysku_report\`
WHERE 
  sale_date BETWEEN @startDate AND @endDate
  {{where_clause}}
GROUP BY 
  sale_date
ORDER BY 
  sale_date asc;
`;

module.exports = {
	buyBoxPercentageTrendQuery,
	trafficProductPerformanceTableQuery,
	trafficSessionsPageviewsQuery,
};
