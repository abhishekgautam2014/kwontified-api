// index.js
const express = require('express');
const bigQueryRateLimiter = require('./rateLimiter');
const { BigQuery } = require('@google-cloud/bigquery');
require('dotenv').config();

const app = express();
app.use(bigQueryRateLimiter);
const port = process.env.PORT || 3000;

// Initialize BigQuery client
const bigquery = new BigQuery({
  projectId: process.env.PROJECT_ID,
  keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
});

// Test API to check connection
app.get('/test-connection', async (req, res) => {
  try {
    // Get list of datasets as a simple test query
    const [datasets] = await bigquery.getDatasets();
    const datasetNames = datasets.map(ds => ds.id);
    res.json({
      success: true,
      message: 'Connected to BigQuery successfully!',
      datasets: datasetNames,
    });
  } catch (error) {
    console.error('BigQuery connection error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to connect to BigQuery',
      error: error.message,
    });
  }
});

app.get('/all-tables-and-columns', async (req, res) => {
  try {
    const [datasets] = await bigquery.getDatasets();
    const result = [];

    for (const dataset of datasets) {
      const datasetId = dataset.id;
      // console.log(`Processing dataset: ${datasetId}`);
      const [tables] = await bigquery.dataset(datasetId).getTables();
      if(datasetId === "intentwise_ecommerce_graph"){
        // result.push(tables)
        // console.log(`Fetched ${tables} tables from dataset ${datasetId}`);
        const tablesData = [];

        for (const table of tables) {
          const tableId = table.id;
          const [metadata] = await table.getMetadata();
          const schema = metadata.schema;
          const columns = schema.fields.map(field => ({
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
    console.error('Error fetching tables and columns:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch tables and columns',
      error: error.message,
    });
  }
});

app.get('/api/account-summary/metrices', async (req, res) => {
  try {
     const query = `SELECT SUM(ad_spend) AS ad_spend,
     SUM(ordered_revenue) AS ad_revenue,
     SUM(product_sales) AS total_sales
FROM intentwise_ecommerce_graph.account_summary
WHERE report_date BETWEEN '2025-10-01' AND '2025-10-10'`;

    const options = {
      query: query,
      location: 'us-west2',
    };

    const [job] = await bigquery.createQueryJob(options);
    const [rows] = await job.getQueryResults();

    res.json({
      success: true,
      data: rows[0],
    });
  } catch (error) {
    console.error('Error fetching account summary count:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch account summary count',
      error: error.message,
    });
  }
});

async function getDatasetLocation() {
  const datasetId = 'amazon_source_data';
  const [dataset] = await bigquery.dataset(datasetId).get();

  console.log(`Dataset: ${dataset.id}`);
  console.log(`Location: ${dataset.metadata.location}`);
}

// getDatasetLocation();

// Start server
app.listen(port, () => {
  console.log(`✅ Server is running on http://localhost:${port}`);
});
