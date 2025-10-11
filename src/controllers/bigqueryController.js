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

const runQuery = async (req, res) => {
	try {
		const {
			bquery,
			startDate = "2025-10-01",
			endDate = "2025-10-10",
		} = req.query;

		if (!bquery || !queries[bquery]) {
			return res.status(400).json({
				success: false,
				message: "Invalid or missing query parameter",
			});
		}

		const options = {
			query: queries[bquery],
			params: {
				startDate,
				endDate,
			},
			location: process.env.PROJECT_LOCATION,
		};

		const [job] = await bigquery.createQueryJob(options);
		const [rows] = await job.getQueryResults();

		res.json({
			success: true,
			data: rows,
		});
	} catch (error) {
		console.error(`Error running query ${req.query.bquery}:`, error);
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
