const orderFulfillment = `
SELECT 
  fulfillment_channel, count(fulfillment_channel) AS total_orders
FROM 
amazon_source_data.sellercentral_ordersbydate_report 
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
FROM 
amazon_source_data.sellercentral_salesandtrafficbysku_report 
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
FROM 
amazon_source_data.sellercentral_salesandtrafficbydate_report 
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
`;

module.exports = {
    orderFulfillment,
    orderedProductPerformance,
    totalUnitsOrderSales,
    orderDashboard: getOrderDashboardQuery,
};
