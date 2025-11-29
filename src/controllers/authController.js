const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const db = require("../../db");

const login = (req, res) => {
	const { username, password } = req.body;
	console.log("Login attempt for user:", username);
	// Basic input validation
	if (!username || !password) {
		return res
			.status(400)
			.json({ message: "Username and password are required." });
	}

	// Fetch user from the database
	db.get(
		"SELECT * FROM users WHERE username = ?",
		[username],
		(err, user) => {
			if (err) {
				return res
					.status(500)
					.json({ message: "Internal server error." });
			}

			if (!user) {
				return res
					.status(401)
					.json({ message: "Invalid username or password." });
			}

			// Compare the provided password with the stored hash
			bcrypt.compare(password, user.password, (err, isMatch) => {
				if (err) {
					return res
						.status(500)
						.json({ message: "Internal server error." });
				}

				if (!isMatch) {
					return res
						.status(401)
						.json({ message: "Invalid username or password." });
				}

				// If the password is correct, create a JWT
				const token = jwt.sign(
					{ id: user.id, username: user.username },
					process.env.JWT_SECRET,
					{
						expiresIn: "24h", // Token expires in 1 hour
					}
				);

				res.status(200).json({ message: "Login successful.", token });
			});
		}
	);
};

const register = (req, res) => {
	const { username, password } = req.body;

	if (!username || !password) {
		return res
			.status(400)
			.json({ message: "Username and password are required." });
	}

	db.get(
		"SELECT * FROM users WHERE username = ?",
		[username],
		(err, user) => {
			if (err) {
				return res
					.status(500)
					.json({ message: "Internal server error." });
			}

			if (user) {
				return res
					.status(400)
					.json({ message: "Username already exists." });
			}

			bcrypt.hash(password, 10, (err, hash) => {
				if (err) {
					return res
						.status(500)
						.json({ message: "Internal server error." });
				}

				db.run(
					"INSERT INTO users (username, password) VALUES (?, ?)",
					[username, hash],
					(err) => {
						if (err) {
							return res
								.status(500)
								.json({ message: "Internal server error." });
						}

						res.status(201).json({
							message: "User created successfully.",
						});
					}
				);
			});
		}
	);
};

// ----------------------- CHANGE PASSWORD -----------------------
const changePassword = (req, res) => {
	const userId = req.user.id; // comes from JWT middleware
	const { oldPassword, newPassword } = req.body;

	if (!oldPassword || !newPassword) {
		return res
			.status(400)
			.json({ message: "Old & new password are required." });
	}

	// 1. Fetch user from DB
	db.get("SELECT * FROM users WHERE id = ?", [userId], (err, user) => {
		if (err)
			return res.status(500).json({ message: "Internal server error." });
		if (!user) return res.status(404).json({ message: "User not found." });

		// 2. Compare old password
		bcrypt.compare(oldPassword, user.password, (err, isMatch) => {
			if (err)
				return res
					.status(500)
					.json({ message: "Internal server error." });
			if (!isMatch)
				return res
					.status(401)
					.json({ message: "Old password is incorrect." });

			// 3. Hash new password
			bcrypt.hash(newPassword, 10, (err, hash) => {
				if (err)
					return res
						.status(500)
						.json({ message: "Internal server error." });

				// 4. Update password in DB
				db.run(
					"UPDATE users SET password = ? WHERE id = ?",
					[hash, userId],
					(err) => {
						if (err)
							return res
								.status(500)
								.json({ message: "Internal server error." });

						res.status(200).json({
							message: "Password updated successfully.",
						});
					}
				);
			});
		});
	});
};

module.exports = {
	login,
	register,
	changePassword,
};
