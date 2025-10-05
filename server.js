// index.js
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
require('dotenv').config();

const app = express();
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

// Start server
app.listen(port, () => {
  console.log(`✅ Server is running on http://localhost:${port}`);
});
