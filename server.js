const express = require("express");
const cors = require("cors");
const db = require("./db");
const bigQueryRateLimiter = require("./src/middleware/rateLimiter");
const bigqueryRoutes = require("./src/routes/bigqueryRoutes");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(bigQueryRateLimiter);
const port = process.env.PORT || 3000;

app.use("/", bigqueryRoutes);

// GET USER BY ID
// http://localhost:3000/user/1314390
app.get("/user/:id", (req, res) => {
	const id = req.params.id;

	db.get(
		"SELECT account_id, account_name FROM users WHERE account_id = ?",
		[id],
		(err, row) => {
			if (err) return res.status(500).json({ error: err.message });

			if (!row)
				return res.status(404).json({ message: "User not found" });

			res.json(row);
		}
	);
});

// Start server
app.listen(port, () => {
	console.log(`✅ Server is running on http://localhost:${port}`);
});
