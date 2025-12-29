const express = require("express");
const bcrypt = require("bcryptjs");
const AdminMember = require("../models/adminPanel");

const router = express.Router();

const VALID_ROLES = [
  "Manager",
  "Disbursement Officer",
  "Support/Reconciliation Officer",
  "Agency Manager",
];

const serializeMember = (member) => {
  if (!member) {
    return null;
  }
  const data = member.toObject();
  delete data.password;
  return data;
};

router.get("/api/admin-members", async (_req, res) => {
  try {
    const members = await AdminMember.find().sort({ createdAt: -1 }).select("-password");
    return res.json(members);
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch admin members" });
  }
});

router.post("/api/admin-members", async (req, res) => {
  try {
    const {
      firstName,
      lastName,
      email,
      phone,
      password,
      assignedRole,
      gender,
    } = req.body || {};

    if (
      !firstName ||
      !lastName ||
      !email ||
      !phone ||
      !password ||
      !assignedRole ||
      !gender
    ) {
      return res.status(400).json({ message: "All fields are required" });
    }

    if (!VALID_ROLES.includes(assignedRole)) {
      return res.status(400).json({ message: "Invalid role provided" });
    }

    const existing = await AdminMember.findOne({ email: email.toLowerCase() });
    if (existing) {
      return res.status(409).json({ message: "An admin already exists with this email" });
    }

    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(String(password), salt);

    const created = await AdminMember.create({
      firstName,
      lastName,
      email,
      phone,
      password: hashedPassword,
      assignedRole,
      gender,
    });

    return res.status(201).json(serializeMember(created));
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to create admin member" });
  }
});

router.patch("/api/admin-members/:id/suspend", async (req, res) => {
  try {
    const updated = await AdminMember.findByIdAndUpdate(
      req.params.id,
      { $set: { isSuspended: true } },
      { new: true }
    );

    if (!updated) {
      return res.status(404).json({ message: "Admin member not found" });
    }

    return res.json(serializeMember(updated));
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to suspend admin member" });
  }
});

router.patch("/api/admin-members/:id/activate", async (req, res) => {
  try {
    const updated = await AdminMember.findByIdAndUpdate(
      req.params.id,
      { $set: { isSuspended: false } },
      { new: true }
    );

    if (!updated) {
      return res.status(404).json({ message: "Admin member not found" });
    }

    return res.json(serializeMember(updated));
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to activate admin member" });
  }
});

router.delete("/api/admin-members/:id", async (req, res) => {
  try {
    const deleted = await AdminMember.findByIdAndDelete(req.params.id);

    if (!deleted) {
      return res.status(404).json({ message: "Admin member not found" });
    }

    return res.json({ message: "Admin member deleted" });
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to delete admin member" });
  }
});

module.exports = router;
