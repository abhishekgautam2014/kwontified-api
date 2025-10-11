const rateLimit = require('express-rate-limit');

const bigQueryRateLimiter = rateLimit({
  windowMs: 10 * 1000, // 10 seconds
  max: 5, // Limit each IP to 5 requests per windowMs
  message: 'Too many requests for table metadata updates, please try again after 10 seconds',
  headers: true,
});

module.exports = bigQueryRateLimiter;
