const express = require("express");
const cors = require("cors");
const db = require("./db");
const bigQueryRateLimiter = require("./src/middleware/rateLimiter");
const bigqueryRoutes = require("./src/routes/bigqueryRoutes");
const userRoutes = require("./src/routes/userRoutes");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(express.json());
app.use(bigQueryRateLimiter);
const port = process.env.PORT || 3000;

app.use("/", bigqueryRoutes);
app.use("/", userRoutes);



// Start server
app.listen(port, () => {
	console.log(`✅ Server is running on http://localhost:${port}`);
});
