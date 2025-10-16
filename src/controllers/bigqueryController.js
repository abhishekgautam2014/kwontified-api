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

const runQuery = async (req, res) => {
	try {
		const {
			queryName,
			startDate = "2025-10-01",
			endDate = "2025-10-10",
			page = 1,
			pageSize = 10,
			...filters
		} = req.query;

		if (!queryName || !queries[queryName]) {
			return res.status(400).json({
				success: false,
				message: "Invalid or missing query name parameter.",
			});
		}

		const limit = parseInt(pageSize, 10);
		const offset = (parseInt(page, 10) - 1) * limit;

		// 🧩 Step 1: Detect table name (expect backtick-wrapped)
		const queryText = queries[queryName];
		const tableMatch = queryText.match(/`([^`]+)`/);

		if (!tableMatch) {
			return res.status(400).json({
				success: false,
				message:
					"Unable to detect table name in query. Ensure it's wrapped in backticks.",
			});
		}

		const tableId = tableMatch[1]; // e.g. intentwise_ecommerce_graph.product_summary

		// 🧩 Step 2: Get schema dynamically (cached)
		const fields = await getSchema(tableId);
		const validColumns = fields.map((f) => f.name);

		let whereClause = "";
		const params = { startDate, endDate, limit, offset };

		// 🧩 Step 3: Add filters dynamically for only valid columns
		Object.entries(filters).forEach(([key, value]) => {
			if (
				!value ||
				[
					"queryName",
					"startDate",
					"endDate",
					"page",
					"pageSize",
				].includes(key)
			)
				return;

			if (!validColumns.includes(key)) return; // skip invalid filters

			const fieldType =
				fields.find((f) => f.name === key)?.type || "STRING";

			if (fieldType === "STRING") {
				whereClause += ` AND ${key} LIKE @${key}`;
				params[key] = `%${value}%`;
			} else {
				whereClause += ` AND ${key} = @${key}`;
				params[key] = isNaN(value) ? value : Number(value);
			}
		});

		// Replace where placeholder
		const finalQuery = queryText.replace("{{where_clause}}", whereClause);

		const options = {
			query: finalQuery,
			params,
			location: process.env.PROJECT_LOCATION,
		};

		// 🧩 Step 4: Log query in development
		if (process.env.NODE_ENV !== "production") {
			console.log("\n--- BigQuery Debug Log ---");
			console.log("Query Name:", queryName);
			console.log("Table:", tableId);
			console.log("Generated SQL:\n", finalQuery);
			console.log("Bound Parameters:", JSON.stringify(params, null, 2));
			console.log("----------------------------\n");
		}

		// 🧩 Step 5ß: Execute query
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
