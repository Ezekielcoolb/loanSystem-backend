const express = require("express");
const jwt = require("jsonwebtoken");
const Admin = require("../models/Admin");
const jwtSecret = require("../config/jwtSecret");

const router = express.Router();

function createToken(admin) {
  return jwt.sign({ id: admin._id, email: admin.email }, jwtSecret, {
    expiresIn: "7d",
  });
}

router.post("/api/admin/register", async (req, res) => {
  try {
    const { email, password } = req.body || {};

    if (!email || !password) {
      return res.status(400).json({ message: "Email and password are required" });
    }

    const normalizedEmail = email.toLowerCase().trim();
    let admin = await Admin.findOne({ email: normalizedEmail }).select("+password");

    if (admin) {
      admin.password = password;
      await admin.save();
    } else {
      admin = await Admin.create({ email: normalizedEmail, password });
    }

    const token = createToken(admin);

    return res.status(admin.createdAt ? 201 : 200).json({
      token,
      admin: { id: admin._id, email: admin.email },
    });
  } catch (error) {
    return res.status(500).json({ message: error.message || "Unable to register admin" });
  }
});

router.post("/api/admin/login", async (req, res) => {
  try {
    const { email, password } = req.body || {};

    if (!email || !password) {
      return res.status(400).json({ message: "Email and password are required" });
    }

    const admin = await Admin.findOne({ email: email.toLowerCase().trim() }).select("+password");

    if (!admin) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const isMatch = await admin.comparePassword(password);

    if (!isMatch) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const token = createToken(admin);

    return res.json({
      token,
      admin: { id: admin._id, email: admin.email },
    });
  } catch (error) {
    return res.status(500).json({ message: error.message || "Unable to login" });
  }
});

router.get("/api/admin/me", async (req, res) => {
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

    return res.json({ id: admin._id, email: admin.email });
  } catch (error) {
    if (error.name === "JsonWebTokenError" || error.name === "TokenExpiredError") {
      return res.status(401).json({ message: "Invalid token" });
    }

    return res.status(500).json({ message: error.message || "Unable to fetch admin profile" });
  }
});

module.exports = router;
