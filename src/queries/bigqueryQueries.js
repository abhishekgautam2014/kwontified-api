// This file contains all the BigQuery queries used in the application.

module.exports = {
	getAccountSummaryMetrices: `
SELECT 
  IFNULL(SUM(ad_spend), 0) AS ad_spend,
  IFNULL(SUM(ad_revenue), 0) AS ad_revenue,
  IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0) AS total_sales,
  IFNULL(SUM(product_quantity), 0) + IFNULL(SUM(ordered_units), 0) AS total_units_ordered,

  CASE 
    WHEN (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0)) - IFNULL(SUM(ad_revenue), 0) > 0 
      THEN (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0)) - IFNULL(SUM(ad_revenue), 0)
    ELSE 0 
  END AS organic_sales,

  SAFE_DIVIDE(SUM(ad_revenue), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS ad_sales_attrib,

  SAFE_DIVIDE(SUM(ad_spend), SUM(ad_revenue)) AS acos,
  SAFE_DIVIDE(SUM(ad_revenue), SUM(ad_spend)) AS roas,
  SAFE_DIVIDE(SUM(ad_spend), (IFNULL(SUM(product_sales), 0) + IFNULL(SUM(ordered_revenue), 0))) AS tacos

FROM 
  intentwise_ecommerce_graph.account_summary

WHERE 
  report_date BETWEEN @startDate AND @endDate;
`,
};
