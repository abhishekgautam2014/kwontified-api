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

module.exports = {
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
