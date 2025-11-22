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

module.exports = {
	impressionsClicksTrend,
	cpcwithPrevMonthCpcTrend,
	capmpaignPerformanceTable,
	capmpaignSummaryTable,
	sponsoredProductPerformanceTable,
};
