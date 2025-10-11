const express = require('express');
const router = express.Router();
const bigQueryController = require('../controllers/bigqueryController');

router.get('/test-connection', bigQueryController.testConnection);
router.get('/all-tables-and-columns', bigQueryController.getAllTablesAndColumns);
router.get('/account-summary/metrices', bigQueryController.getAccountSummaryMetrices);

module.exports = router;
