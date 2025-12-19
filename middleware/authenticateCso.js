const jwt = require("jsonwebtoken");
const CSO = require("../models/cso");

async function authenticateCso(req, res, next) {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader?.startsWith("Bearer ")) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    const token = authHeader.split(" ")[1];
    const payload = jwt.verify(token, process.env.JWT_SECRET);

    const cso = await CSO.findById(payload.id);

    if (!cso) {
      return res.status(401).json({ message: "Invalid token" });
    }

    req.cso = cso;
    next();
  } catch (error) {
    if (error.name === "JsonWebTokenError" || error.name === "TokenExpiredError") {
      return res.status(401).json({ message: "Invalid token" });
    }
    return res.status(500).json({ message: "Unable to authenticate" });
  }
}

module.exports = authenticateCso;
