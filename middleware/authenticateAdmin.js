const jwt = require("jsonwebtoken");
const Admin = require("../models/Admin");
const jwtSecret = require("../config/jwtSecret");

async function authenticateAdmin(req, res, next) {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader?.startsWith("Bearer ")) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    const token = authHeader.split(" ")[1];
    const payload = jwt.verify(token, jwtSecret);

    const admin = await Admin.findById(payload.id);

    if (!admin) {
      return res.status(401).json({ message: "Invalid token" });
    }

    req.admin = admin;
    next();
  } catch (error) {
    if (
      error.name === "JsonWebTokenError" ||
      error.name === "TokenExpiredError"
    ) {
      return res.status(401).json({ message: "Invalid token" });
    }
    return res.status(500).json({ message: "Unable to authenticate" });
  }
}

module.exports = authenticateAdmin;
