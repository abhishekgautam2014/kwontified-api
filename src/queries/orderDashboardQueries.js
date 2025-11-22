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

// Order | Shipped Orders Queries

const orderShipmentStatusList = `
SELECT 
FORMAT_DATE('%Y-%m-%d', purchase_date) AS purchase_date, amazon_order_id, order_status, fulfillment_channel, item_status, product_name, sku, asin,
CAST(sum(quantity) AS FLOAT64) AS quantity, 
CAST(sum(item_price) AS FLOAT64) AS item_price, 
CAST(sum(item_promotion_discount) AS FLOAT64) as item_promotion_discount, 
CAST(sum(item_tax) AS FLOAT64) as item_tax
FROM 
  \`amazon_source_data.sellercentral_ordersbydate_report\`
WHERE 
  purchase_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
GROUP BY 
  purchase_date, amazon_order_id, order_status, fulfillment_channel, item_status, product_name, sku, asin
ORDER BY 
  purchase_date DESC
`;

const orderShipmentStatus = `
SELECT 
count(*) as order_count, order_status
FROM 
  \`amazon_source_data.sellercentral_ordersbydate_report\`
WHERE 
  purchase_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
GROUP BY 
  order_status
`;

const totalUnitsOrdered = `
SELECT 
  sum(units_ordered) AS units_ordered,
  sum(total_order_items) AS total_order_items,
  CAST(sum(ordered_product_sales_amt) AS FLOAT64)  AS total_sales,
  FORMAT_DATE('%Y-%m-%d', sale_date) AS sale_date
FROM 
  \`amazon_source_data.sellercentral_salesandtrafficbydate_report\`
WHERE 
  sale_date BETWEEN @startDate AND @endDate
  AND account_id = @account_id
GROUP BY 
  sale_date
`;

module.exports = {
	orderFulfillment,
	orderedProductPerformance,
	totalUnitsOrderSales,
	orderShipmentStatusList,
	orderShipmentStatus,
	totalUnitsOrdered,
};
