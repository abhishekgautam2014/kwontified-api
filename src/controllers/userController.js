const db = require("./../../db");
const bcrypt = require("bcrypt");
const saltRounds = 10;

exports.getAllUsers = (req, res) => {
	db.all(
		"SELECT id, account_id, account_name, username, email, name, description FROM users",
		[],
		(err, rows) => {
			if (err) return res.status(500).json({ error: err.message });
			res.json({ users: rows });
		}
	);
};

exports.getUserById = (req, res) => {
	const id = req.params.id;
	db.get(
		"SELECT id, account_id, account_name, username, email, name, description FROM users WHERE id = ?",
		[id],
		(err, row) => {
			if (err) return res.status(500).json({ error: err.message });
			if (!row)
				return res.status(404).json({ message: "User not found" });
			res.json(row);
		}
	);
};

exports.createUser = (req, res) => {
	const {
		account_id,
		account_name,
		username,
		email,
		name,
		description,
		password,
	} = req.body;
	if (
		!account_id ||
		!account_name ||
		!username ||
		!email ||
		!name ||
		!description ||
		!password
	) {
		return res
			.status(400)
			.json({ error: "Please provide all required fields." });
	}

	bcrypt.hash(password, saltRounds, (err, hash) => {
		if (err)
			return res.status(500).json({ error: "Error hashing password." });

		const stmt = db.prepare(
			"INSERT INTO users (account_id, account_name, username, email, name, description, password) VALUES (?, ?, ?, ?, ?, ?, ?)"
		);
		stmt.run(
			account_id,
			account_name,
			username,
			email,
			name,
			description,
			hash,
			function (err) {
				if (err) return res.status(500).json({ error: err.message });
				res.status(201).json({ id: this.lastID });
			}
		);
		stmt.finalize();
	});
};

exports.updateUser = (req, res) => {
	const id = req.params.id;
	const { account_id, account_name, username, email, name, description } =
		req.body;
	if (
		!account_id &&
		!account_name &&
		!username &&
		!email &&
		!name &&
		!description
	) {
		return res
			.status(400)
			.json({ error: "Please provide at least one field to update." });
	}

	let fields = [];
	let values = [];
	if (account_id) {
		fields.push("account_id = ?");
		values.push(account_id);
	}
	if (account_name) {
		fields.push("account_name = ?");
		values.push(account_name);
	}
	if (username) {
		fields.push("username = ?");
		values.push(username);
	}
	if (email) {
		fields.push("email = ?");
		values.push(email);
	}
	if (name) {
		fields.push("name = ?");
		values.push(name);
	}
	if (description) {
		fields.push("description = ?");
		values.push(description);
	}
	values.push(id);

	const stmt = db.prepare(
		`UPDATE users SET ${fields.join(", ")} WHERE id = ?`
	);
	stmt.run(values, function (err) {
		if (err) return res.status(500).json({ error: err.message });
		if (this.changes === 0)
			return res.status(404).json({ message: "User not found" });
		res.json({ message: "User updated successfully." });
	});
	stmt.finalize();
};

exports.deleteUser = (req, res) => {
	const id = req.params.id;
	const stmt = db.prepare("DELETE FROM users WHERE id = ?");
	stmt.run(id, function (err) {
		if (err) return res.status(500).json({ error: err.message });
		if (this.changes === 0)
			return res.status(404).json({ message: "User not found" });
		res.json({ message: "User deleted successfully." });
	});
	stmt.finalize();
};
