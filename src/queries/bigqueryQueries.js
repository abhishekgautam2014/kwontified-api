// This file contains all the BigQuery queries used in the application.

const accountSummaryMetrices = `
SELECT 
  -- Core spend & sales metrics
  IFNULL(SUM(ad_spend), 0) AS ad_spend,
  IFNULL(SUM(ad_revenue), 0) AS ad_revenue,
  IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0) AS total_sales,

  -- Derived sales metrics
  CASE 
    WHEN (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0)) - IFNULL(SUM(ad_revenue), 0) > 0 
      THEN (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0)) - IFNULL(SUM(ad_revenue), 0)
    ELSE 0 
  END AS organic_sales,

  SAFE_DIVIDE(SUM(ad_revenue), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS ad_sales_attrib,
  SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS acos,
  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
  SAFE_DIVIDE(SUM(ad_spend), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS tacos,

  SAFE_DIVIDE((IFNULL(AVG(product_sales), 0) + IFNULL(AVG(ordered_revenue), 0)), COUNT(DISTINCT report_date)) AS avg_daily_sales,
  SAFE_DIVIDE((IFNULL(AVG(product_quantity), 0) + IFNULL(AVG(ordered_units), 0)), COUNT(DISTINCT report_date)) AS avg_daily_units
FROM 
  \`intentwise_ecommerce_graph.account_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate;
`;

const selleCentralMetrices = `
SELECT 
  -- Core spend & sales metrics
SUM(units_ordered) AS units_ordered,
  SUM(total_order_items) AS total_order_items,
  AVG(avg_sales_per_order_item_amt) AS avg_sales_per_order_item_amt,
  AVG(avg_selling_price_amt) AS avg_selling_price_amt,
  SUM(sessions) AS sessions,
  SUM(page_views) AS page_views,
  AVG(avg_offer_count) AS avg_offer_count,
  AVG(refund_rate) AS refund_rate
FROM 
  \`amazon_source_data.sellercentral_salesandtrafficbydate_report\`
WHERE 
  sale_date BETWEEN @startDate AND @endDate;
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
GROUP BY report_date
ORDER BY report_date;
`;

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

const productBySales = `
SELECT 
  product AS asin,
  sku,
  SAFE_ADD(SUM(product_sales), SUM(ordered_revenue)) AS total_sales,
  IFNULL(SUM(ad_revenue), 0) AS ad_revenue,
  IFNULL(SUM(ad_spend), 0) AS ad_spend
FROM 
  intentwise_ecommerce_graph.product_summary
WHERE 
  report_date BETWEEN @startDate AND @endDate
GROUP BY 
  asin, sku
ORDER BY 
  total_sales DESC
LIMIT 100;
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
  ) AS tacos

FROM 
  \`intentwise_ecommerce_graph.product_summary\`

WHERE 
  report_date BETWEEN @startDate AND @endDate

GROUP BY 
  product_title, sku, product

ORDER BY 
  total_sales DESC

LIMIT @limit OFFSET @offset;
`;

module.exports = {
	accountSummaryMetrices: accountSummaryMetrices,
	addRevenueTotalSalesTrend: addRevenueTotalSalesTrend,
	selleCentralMetrices: selleCentralMetrices,
	acosTacosTrend: acosTacosTrend,
	productSummary: productSummary,
	acosTacosTrend: acosTacosTrend,
	productBySales: productBySales,
};
