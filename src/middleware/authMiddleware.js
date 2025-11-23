require("dotenv").config();
const jwt = require("jsonwebtoken");
const db = require("../../db");

const protect = (req, res, next) => {
  let token;

  if (
    req.headers.authorization &&
    req.headers.authorization.startsWith("Bearer")
  ) {
    try {
      token = req.headers.authorization.split(" ")[1];
      const decoded = jwt.verify(token, process.env.JWT_SECRET);

      db.get(
        "SELECT * FROM users WHERE id = ?",
        [decoded.id],
        (err, user) => {
          if (err) {
            return res.status(500).json({ message: "Internal server error" });
          }
          if (!user) {
            return res.status(401).json({ message: "Not authorized, token failed" });
          }
          req.user = user;
          next();
        }
      );
    } catch (error) {
      res.status(401).json({ message: "Not authorized, token failed" });
    }
  }

  if (!token) {
    res.status(401).json({ message: "Not authorized, no token" });
  }
};

module.exports = { protect };
