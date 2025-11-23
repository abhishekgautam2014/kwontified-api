require("dotenv").config();
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const db = require("../../db");

const register = (req, res) => {
  const { account_name, password } = req.body;

  if (!account_name || !password) {
    return res
      .status(400)
      .json({ message: "Please provide account_name and password" });
  }

  db.get(
    "SELECT * FROM users WHERE account_name = ?",
    [account_name],
    (err, user) => {
      if (err) {
        return res.status(500).json({ message: "Internal server error" });
      }
      if (user) {
        return res.status(400).json({ message: "User already exists" });
      }

      const salt = bcrypt.genSaltSync(10);
      const hashedPassword = bcrypt.hashSync(password, salt);

      db.run(
        "INSERT INTO users (account_name, password) VALUES (?, ?)",
        [account_name, hashedPassword],
        (err) => {
          if (err) {
            return res.status(500).json({ message: "Internal server error" });
          }
          res.status(201).json({ message: "User registered successfully" });
        }
      );
    }
  );
};

const login = (req, res) => {
  const { account_name, password } = req.body;

  if (!account_name || !password) {
    return res
      .status(400)
      .json({ message: "Please provide account_name and password" });
  }

  db.get(
    "SELECT * FROM users WHERE account_name = ?",
    [account_name],
    (err, user) => {
      if (err) {
        return res.status(500).json({ message: "Internal server error" });
      }
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }

      const isPasswordCorrect = bcrypt.compareSync(password, user.password);
      if (!isPasswordCorrect) {
        return res.status(401).json({ message: "Invalid credentials" });
      }

      const token = jwt.sign({ id: user.id }, process.env.JWT_SECRET, {
        expiresIn: "30d",
      });

      res.status(200).json({ token });
    }
  );
};

module.exports = {
  register,
  login,
};
