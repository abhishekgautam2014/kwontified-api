const { BigQuery } = require("@google-cloud/bigquery");
const queries = require("../queries/bigqueryQueries");
require("dotenv").config();

// Initialize BigQuery client
const bigquery = new BigQuery({
	projectId: process.env.PROJECT_ID,
	keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
});

const testConnection = async (req, res) => {
	try {
		// Get list of datasets as a simple test query
		const [datasets] = await bigquery.getDatasets();
		const datasetNames = datasets.map((ds) => ds.id);
		res.json({
			success: true,
			message: "Connected to BigQuery successfully!",
			datasets: datasetNames,
		});
	} catch (error) {
		console.error("BigQuery connection error:", error);
		res.status(500).json({
			success: false,
			message: "Failed to connect to BigQuery",
			error: error.message,
		});
	}
};

const getAllTablesAndColumns = async (req, res) => {
	try {
		const [datasets] = await bigquery.getDatasets();
		const result = [];

		for (const dataset of datasets) {
			const datasetId = dataset.id;
			const [tables] = await bigquery.dataset(datasetId).getTables();
			if (datasetId === "intentwise_ecommerce_graph") {
				const tablesData = [];

				for (const table of tables) {
					const tableId = table.id;
					const [metadata] = await table.getMetadata();
					const schema = metadata.schema;
					const columns = schema.fields.map((field) => ({
						name: field.name,
						type: field.type,
						mode: field.mode,
						description: field.description,
					}));
					tablesData.push({ table: tableId, columns });
				}
				result.push({ dataset: datasetId, tables: tablesData });
			}
		}

		res.json({
			success: true,
			data: result,
		});
	} catch (error) {
		console.error("Error fetching tables and columns:", error);
		res.status(500).json({
			success: false,
			message: "Failed to fetch tables and columns",
			error: error.message,
		});
	}
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
const buildFilters = (filters, validColumns, fields) => {
	let whereClause = "";
	const params = {};

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
		const direction = ["asc", "desc"].includes(sortOrder?.toLowerCase())
			? sortOrder.toUpperCase()
			: "DESC";
		return `ORDER BY ${sortBy} ${direction}`;
	}
	return " "; // fallback
};

// 🧩 3️⃣ Main Function — runQuery
const runQuery = async (req, res) => {
	try {
		const {
			queryName,
			startDate = "2025-10-01",
			endDate = "2025-10-10",
			page,
			pageSize,
			sortBy,
			sortOrder = "desc",
			...filters
		} = req.query;

		if (!queryName || !queries[queryName]) {
			return res.status(400).json({
				success: false,
				message: "Invalid or missing query name parameter.",
			});
		}

		const combinedQueryNames = ["timeSeriesMetrics", "accountSummary"];
		if (combinedQueryNames.includes(queryName)) {
			const queryText = queries[queryName];
			const params = { startDate, endDate };

			const options = {
				query: queryText,
				params,
				location: process.env.PROJECT_LOCATION,
			};

			const [job] = await bigquery.createQueryJob(options);
			const [rows] = await job.getQueryResults();

			const responseData = {};
			for (const row of rows) {
				responseData[row.queryName] = JSON.parse(row.results || "[]");
			}

			return res.json({ success: true, data: responseData });
		}

		const queryText = queries[queryName];
		const tableMatch = queryText.match(/`([^`]+)`/);
		if (!tableMatch) {
			return res.status(400).json({
				success: false,
				message:
					"Unable to detect table name in query. Ensure it's wrapped in backticks.",
			});
		}

		const tableId = tableMatch[1];
		const fields = await getSchema(tableId);
		const validColumns = fields.map((f) => f.name);

		// 🔍 Filters
		const { whereClause, params: filterParams } = buildFilters(
			filters,
			validColumns,
			fields
		);

		// 🔽 Sorting
		const orderClause = buildSorting(sortBy, sortOrder, validColumns);

		// 🧠 Merge all params
		const params = {
			startDate,
			endDate,
			...filterParams,
		};

		// 🧩 Final Query Construction
		let finalQuery = queryText
			.replace("{{where_clause}}", whereClause)
			.replace(/ORDER BY[\s\S]*?(?=LIMIT|$)/i, "")
			.replace(/LIMIT[\s\S]*/i, "");

		finalQuery += `\n${orderClause}`;

		// Add LIMIT and OFFSET if page and pageSize are provided
		if (page && pageSize) {
			const limit = parseInt(pageSize, 10);
			const offset = (parseInt(page, 10) - 1) * limit;
			finalQuery += `\nLIMIT @limit OFFSET @offset`;
			params.limit = limit;
			params.offset = offset;
		}

		const options = {
			query: finalQuery,
			params,
			location: process.env.PROJECT_LOCATION,
		};

		// 🧾 Log in Dev Mode
		if (process.env.NODE_ENV !== "production") {
			console.log("\n--- BigQuery Debug Log ---");
			console.log("Query Name:", queryName);
			console.log("Table:", tableId);
			console.log("Generated SQL:\n", finalQuery);
			console.log("Parameters:", JSON.stringify(params, null, 2));
			console.log("----------------------------\n");
		}

		// 🚀 Execute
		const [job] = await bigquery.createQueryJob(options);
		const [rows] = await job.getQueryResults();

		res.json({ success: true, data: rows });
	} catch (error) {
		console.error("Error running query:", error);
		res.status(500).json({
			success: false,
			message: "Failed to execute BigQuery query",
			error: error.message,
		});
	}
};

module.exports = {
	testConnection,
	getAllTablesAndColumns,
	runQuery,
};
