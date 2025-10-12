const rateLimit = require("express-rate-limit");

const bigQueryRateLimiter = rateLimit({
	windowMs: 10 * 1000, // 10 seconds
	max: 10, // Limit each IP to 1 request per windowMs
	message:
		"Too many requests for table metadata updates, please try again after 10 seconds",
	headers: true,
});

module.exports = bigQueryRateLimiter;
