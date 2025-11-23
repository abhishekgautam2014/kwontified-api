const express = require("express");
const router = express.Router();
const bigQueryController = require("../controllers/bigqueryController");

router.get("/", bigQueryController.testConnection);
router.get(
	"/tables",
	bigQueryController.getAllTablesAndColumns
);
router.get("/metrics/:queryName", bigQueryController.runQuery);

module.exports = router;
