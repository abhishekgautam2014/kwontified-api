const { BigQuery } = require("@google-cloud/bigquery");
const queries = require("../queries/bigqueryQueries");
require("dotenv").config();

// Initialize BigQuery client
const bigquery = new BigQuery({
	projectId: process.env.PROJECT_ID,
	keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
});

// In-memory cache
const cache = new Map();

const getFromCache = (key) => {
	const entry = cache.get(key);
	if (entry && entry.expiry > Date.now()) {
		return entry.value;
	}
	if (entry) {
		cache.delete(key);
	}
	return null;
};

const setInCache = (key, value, ttl = 15 * 60 * 1000) => {
	// 15 minutes default TTL
	const expiry = Date.now() + ttl;
	cache.set(key, { value, expiry });
};

const schemaCache = {};

const getSchema = async (tableId) => {
	try {
		if (schemaCache[tableId]) return schemaCache[tableId];

		const [datasetName, tableName] = tableId.split(".");
		if (!datasetName || !tableName) {
			throw new Error(
				`Invalid table identifier: ${tableId}. Expected format dataset.table`
			);
		}

		const table = bigquery.dataset(datasetName).table(tableName);
		const [metadata] = await table.getMetadata();

		const fields = metadata.schema.fields;
		schemaCache[tableId] = fields; // cache for next time
		return fields;
	} catch (err) {
		console.error("Error fetching schema:", err.message);
		throw err;
	}
};

// 🧩 1️⃣ Build WHERE clause dynamically based on filters
const buildFilters = (filters, validColumns, fields, account_id) => {
	let whereClause = " AND account_id = @account_id";
	const params = { account_id: Number(account_id) };

	Object.entries(filters).forEach(([key, value]) => {
		if (
			!value ||
			[
				"queryName",
				"startDate",
				"endDate",
				"page",
				"pageSize",
				"sortBy",
				"sortOrder",
				"account_id",
			].includes(key)
		)
			return;

		if (!validColumns.includes(key)) return;

		const fieldType = fields.find((f) => f.name === key)?.type || "STRING";

		if (fieldType === "STRING") {
			whereClause += ` AND ${key} LIKE @${key}`;
			params[key] = `%${value}%`;
		} else {
			whereClause += ` AND ${key} = @${key}`;
			params[key] = isNaN(value) ? value : Number(value);
		}
	});

	return { whereClause, params };
};

// 🧩 2️⃣ Build ORDER BY clause dynamically
const buildSorting = (sortBy, sortOrder, validColumns) => {
	if (sortBy && validColumns.includes(sortBy)) {
		const direction = ["asc", "desc"].includes(sortOrder?.toLowerCase()) ? sortOrder.toUpperCase() : "DESC";
		return `ORDER BY ${sortBy} ${direction}`;
	}
	return " "; // fallback
};

// 🧩 3️⃣ Main Function — runQuery
const runQuery = async (queryParams) => {
	const {
		queryName,
		startDate = "2025-10-01",
		endDate = "2025-10-10",
		page,
		pageSize,
		sortBy,
		sortOrder = "desc",
		account_id,
		...filters
	} = queryParams;

	if (!account_id) {
		throw new Error("account_id is a required parameter.");
	}

	const cacheKey = JSON.stringify(queryParams);
	const cachedData = getFromCache(cacheKey);
	if (cachedData) {
		return cachedData;
	}

	if (!queryName || !queries[queryName]) {
		throw new Error("Invalid or missing query name parameter.");
	}

	const combinedQueryNames = ["timeSeriesMetrics", "accountSummary", "dashboardMetrics", "advertisingDashboard", "orderDashboard"];
	let finalQuery, params;

	if (combinedQueryNames.includes(queryName)) {
		const accountIdClause = `AND account_id = @account_id`;
		finalQuery = queries[queryName](accountIdClause);
		params = { startDate, endDate, account_id: Number(account_id) };
	} else {
		const queryText = queries[queryName];
		const tableMatch = queryText.match(/`([^`]+)`/);
		if (!tableMatch) {
			throw new Error(
				"Unable to detect table name in query. Ensure it's wrapped in backticks."
			);
		}

		const tableId = tableMatch[1];
		const fields = await getSchema(tableId);
		const validColumns = fields.map((f) => f.name);

		const { whereClause, params: filterParams } = buildFilters(
			filters,
			validColumns,
			fields,
			account_id
		);
		const orderClause = buildSorting(sortBy, sortOrder, validColumns);

		params = {
			startDate,
			endDate,
			...filterParams,
		};

		finalQuery = queryText
			.replace("{{where_clause}}", whereClause)
			.replace(/ORDER BY[\s\S]*?(?=LIMIT|$)/i, "")
			.replace(/LIMIT[\s\S]*/i, "");

		finalQuery += `\n${orderClause}`;

		if (page && pageSize) {
			const limit = parseInt(pageSize, 10);
			const offset = (parseInt(page, 10) - 1) * limit;
			finalQuery += `\nLIMIT @limit OFFSET @offset`;
			params.limit = limit;
			params.offset = offset;
		}
	}

	const options = {
		query: finalQuery,
		params,
		location: process.env.PROJECT_LOCATION,
	};

	if (process.env.NODE_ENV !== "production") {
		console.log("\n--- BigQuery Debug Log ---");
		console.log("Query Name:", queryName);
		console.log("Generated SQL:\n", finalQuery);
		console.log("Parameters:", JSON.stringify(params, null, 2));
		console.log("----------------------------\n");
	}

	const [job] = await bigquery.createQueryJob(options);
	const [rows] = await job.getQueryResults();

	let responseData;
	if (combinedQueryNames.includes(queryName)) {
		responseData = {};
		for (const row of rows) {
			responseData[row.queryName] = JSON.parse(row.results || "[]");
		}
	} else {
		responseData = rows;
	}

	setInCache(cacheKey, responseData);
	return responseData;
};

module.exports = {
	runQuery,
};
