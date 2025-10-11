const express = require('express');
const bigQueryRateLimiter = require('./src/middleware/rateLimiter');
const bigqueryRoutes = require('./src/routes/bigqueryRoutes');
require('dotenv').config();

const app = express();
app.use(bigQueryRateLimiter);
const port = process.env.PORT || 3000;

app.use('/', bigqueryRoutes);

// Start server
app.listen(port, () => {
  console.log(`✅ Server is running on http://localhost:${port}`);
});