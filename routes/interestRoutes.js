const express = require("express");
const Interest = require("../models/NewInterest");

const router = express.Router();

function parseAmount(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

router.get("/api/interest", async (_req, res) => {
  try {
    const interest = await Interest.findOne({}, null, { sort: { createdAt: -1 } }).lean();
    return res.json(interest || null);
  } catch (error) {
    return res.status(500).json({ message: error.message || "Unable to fetch interest" });
  }
});

router.post("/api/interest", async (req, res) => {
  try {
    const amount = parseAmount(req.body.amount);
    const description = typeof req.body.description === "string" ? req.body.description.trim() : "";

    if (!Number.isFinite(amount) || amount < 0) {
      return res.status(400).json({ message: "Provide a valid non-negative amount" });
    }

    if (!description) {
      return res.status(400).json({ message: "Description is required" });
    }

    const updated = await Interest.findOneAndUpdate(
      {},
      { $set: { amount, description } },
      { new: true, upsert: true, setDefaultsOnInsert: true }
    ).lean();

    return res.status(201).json(updated);
  } catch (error) {
    return res.status(400).json({ message: error.message || "Unable to set interest" });
  }
});

module.exports = router;
