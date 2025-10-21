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
    SAFE_DIVIDE(SUM(ad_spend), SUM(ad_clicks)) AS cpc
  FROM \`intentwise_ecommerce_graph.account_summary\`, date_ranges
  WHERE report_date BETWEEN date_ranges.start_date AND date_ranges.end_date
    /* {{account_id_clause}} */
  GROUP BY report_date
),

-- Previous period daily CPC (same date offset last month)
previous_period AS (
  SELECT 
    report_date,
    SAFE_DIVIDE(SUM(ad_spend), SUM(ad_clicks)) AS prev_cpc
  FROM \`intentwise_ecommerce_graph.account_summary\`, date_ranges
  WHERE report_date BETWEEN date_ranges.prev_start_date AND date_ranges.prev_end_date
    /* {{account_id_clause}} */
  GROUP BY report_date
)

-- Final output: join by day offset
SELECT
  FORMAT_DATE('%Y-%m-%d', c.report_date) AS report_date,
  c.cpc AS current_cpc,
  p.prev_cpc AS previous_cpc,
  SAFE_DIVIDE(c.cpc - p.prev_cpc, p.prev_cpc) * 100 AS cpc_change_pct
FROM current_period c
LEFT JOIN previous_period p
  ON DATE_SUB(c.report_date, INTERVAL 1 MONTH) = p.report_date
ORDER BY c.report_date;
`;

const capmpaignPerformanceTable = `
SELECT 
  campaign_type,
  IFNULL(SUM(ad_spend), 0) AS ad_spend,
  IFNULL(SUM(ad_revenue), 0) AS ad_revenue,
  SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS acos,
  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
  SAFE_DIVIDE(SUM(ad_spend), SUM(SUM(ad_spend)) OVER ()) AS ad_spend_percentage,
  SAFE_DIVIDE(SUM(ad_revenue), SUM(SUM(ad_revenue)) OVER ()) AS ad_revenue_percentage
FROM 
  \`intentwise_ecommerce_graph.campaign_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{where_clause}}
GROUP BY 
  campaign_type
`;

const capmpaignSummaryTable = `
SELECT 
  campaign_type,
  IFNULL(SUM(ad_spend), 0) AS ad_spend,
  IFNULL(SUM(ad_revenue), 0) AS ad_revenue,
  SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS acos,
  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
  SAFE_DIVIDE(SUM(ad_spend), SUM(SUM(ad_spend)) OVER ()) AS ad_spend_percentage,
  SAFE_DIVIDE(SUM(ad_revenue), SUM(SUM(ad_revenue)) OVER ()) AS ad_revenue_percentage,
  IFNULL(SUM(ad_impressions), 0) AS ad_impressions,
  IFNULL(SUM(ad_clicks), 0) AS ad_clicks,
  SAFE_DIVIDE(SUM(ad_clicks), SUM(ad_impressions)) * 100 AS ctr,
  SAFE_DIVIDE(SUM(ad_spend), SUM(ad_clicks)) AS avg_cpc
FROM 
  \`intentwise_ecommerce_graph.campaign_summary\`
WHERE 
  report_date BETWEEN @startDate AND @endDate
  {{where_clause}}
GROUP BY 
  campaign_name
`;

const sponsoredProductPerformanceTable = `
SELECT 
  product AS asin,
  product_title,
  sku,
  IFNULL(SUM(ad_revenue), 0) AS ad_revenue,
  IFNULL(SUM(ad_spend), 0) AS ad_spend,
  SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS acos,
  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
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

module.exports = {
	cpcwithPrevMonthCpcTrend,
	impressionsClicksTrend,
	capmpaignPerformanceTable,
	capmpaignSummaryTable,
	sponsoredProductPerformanceTable,
};
