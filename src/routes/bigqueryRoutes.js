const express = require("express");
const router = express.Router();
const bigQueryController = require("../controllers/bigqueryController");
const authenticateToken = require("../middleware/authMiddleware");

router.get(
	"/test-connection",
	authenticateToken,
	bigQueryController.testConnection
);
router.get(
	"/all-tables-and-columns",
	authenticateToken,
	bigQueryController.getAllTablesAndColumns
);
router.get(
	"/account-summary/metrices",
	authenticateToken,
	bigQueryController.runQuery
);

module.exports = router;
