const {
	accountSummaryMetrices,
	selleCentralMetrices,
} = require("./metricesQueries");
const {
	addRevenueTotalSalesTrend,
	acosTacosTrend,
	totalUnitOrderedandSales,
	productBySales,
	productSummary,
	totalSalesByWeek,
	totalSalesByMonth,
	weeklyTotalSales,
	monthlyTotalSales,
} = require("./salesDashboardQueries");

const {
	orderFulfillment,
	orderedProductPerformance,
	totalUnitsOrderSales,
	orderShipmentStatusList,
	orderShipmentStatus,
	totalUnitsOrdered,
} = require("./orderDashboardQueries");
const {
	spendByBrand,
	revenueByBrand,
	roasByBrand,
	brandOverview,
	spendByMatchType,
	revenueByMatchType,
	roasByMatchType,
	matchTypeOverview,
	keywordSummary,
} = require("./advertisingQueries");

const {
	impressionsClicksTrend,
	cpcwithPrevMonthCpcTrend,
	capmpaignPerformanceTable,
	capmpaignSummaryTable,
	sponsoredProductPerformanceTable,
} = require("./campiagnSummaryQueries");

const {
	buyBoxPercentageTrendQuery,
	trafficProductPerformanceTableQuery,
	trafficSessionsPageviewsQuery,
} = require("./trafficQueries");

const { getAllAccounts } = require("./accountSummaryQueries");
// This file contains all the BigQuery queries used in the application.

const getTimeSeriesMetricsQuery = (accountIdClause) => `
    SELECT 'addRevenueTotalSalesTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${addRevenueTotalSalesTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'acosTacosTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${acosTacosTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalUnitOrderedandSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalUnitOrderedandSales
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

const getAccountSummaryQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

const getDashboardMetricsQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'addRevenueTotalSalesTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${addRevenueTotalSalesTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'acosTacosTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${acosTacosTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalUnitOrderedandSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalUnitOrderedandSales
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
	UNION ALL
	SELECT 'productBySales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${productBySales
		.replace("{{where_clause}}", "")
		.replace(/;\s*$/, "")}) AS t
`;

const getYearlyMonthSalesDashboardMetricsQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'addRevenueTotalSalesTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${addRevenueTotalSalesTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'acosTacosTrend' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${acosTacosTrend
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalUnitOrderedandSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalUnitOrderedandSales
		.replace("{{account_id_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
	UNION ALL
	SELECT 'productBySales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${productBySales
		.replace("{{where_clause}}", "")
		.replace(/;\s*$/, "")}) AS t
`;

const getAdvertisingDashboardQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'selleCentralMetrices' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'impressionsClicksTrend' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${impressionsClicksTrend
		.replace("/* {{account_id_clause}} */", accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'cpcwithPrevMonthCpcTrend' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${cpcwithPrevMonthCpcTrend.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'capmpaignPerformanceTable' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${capmpaignPerformanceTable
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'capmpaignSummaryTable' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${capmpaignSummaryTable
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'sponsoredProductPerformanceTable' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${sponsoredProductPerformanceTable
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t

    UNION ALL

    SELECT 'addRevenueTotalSalesTrend' AS queryName, 
           '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results 
    FROM (${addRevenueTotalSalesTrend
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
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
    UNION ALL
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

const get12monthSalesQuery = (accountIdClause) => `
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalSalesByWeek' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalSalesByWeek
		.replace("/* {{account_id_clause}} */", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalSalesByMonth' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalSalesByMonth
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'productSummary' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${productSummary
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'weeklyTotalSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${weeklyTotalSales
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'monthlyTotalSales' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${monthlyTotalSales
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

const getAdvertisingTargetingAnalysisQuery = (accountIdClause) => `
    SELECT 'spendByBrand' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${spendByBrand
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'revenueByBrand' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${revenueByBrand
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'roasByBrand' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${roasByBrand
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'brandOverview' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${brandOverview
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'spendByMatchType' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${spendByMatchType
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'revenueByMatchType' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${revenueByMatchType
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'roasByMatchType' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${roasByMatchType
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'matchTypeOverview' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${matchTypeOverview
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'keywordSummary' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${keywordSummary
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

const getShippmentOrderDashboardQuery = (accountIdClause) => `
    SELECT 'orderShipmentStatusList' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${orderShipmentStatusList
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'orderShipmentStatus' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${orderShipmentStatus
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'totalUnitsOrdered' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${totalUnitsOrdered
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

const getTrafficDashboardQuery = (accountIdClause) => `
    SELECT 'buyBoxPercentageTrendQuery' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${buyBoxPercentageTrendQuery
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'trafficProductPerformanceTableQuery' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${trafficProductPerformanceTableQuery
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'trafficSessionsPageviewsQuery' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${trafficSessionsPageviewsQuery
		.replace("{{where_clause}}", accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'accountSummaryMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${accountSummaryMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
    UNION ALL
    SELECT 'selleCentralMetrices' AS queryName, '[' || ARRAY_TO_STRING(ARRAY_AGG(TO_JSON_STRING(t)), ',') || ']' AS results FROM (${selleCentralMetrices
		.replace(/{{account_id_clause}}/g, accountIdClause)
		.replace(/;\s*$/, "")}) AS t
`;

module.exports = {
	timeSeriesMetrics: getTimeSeriesMetricsQuery,
	accountSummary: getAccountSummaryQuery,
	dashboardMetrics: getDashboardMetricsQuery,
	yearlyMonthSalesDashboardMetrics: getYearlyMonthSalesDashboardMetricsQuery,
	advertisingDashboard: getAdvertisingDashboardQuery,
	orderDashboard: getOrderDashboardQuery,
	"12monthSales": get12monthSalesQuery,
	advertisingTargetingAnalysis: getAdvertisingTargetingAnalysisQuery,
	shippmentOrderDashboard: getShippmentOrderDashboardQuery,
	trafficDashboard: getTrafficDashboardQuery,
	buyBoxPercentageTrend: buyBoxPercentageTrendQuery,
	trafficProductPerformanceTable: trafficProductPerformanceTableQuery,
	trafficSessionsPageviews: trafficSessionsPageviewsQuery,
	allAccounts: getAllAccounts,
};
