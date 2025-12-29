const express = require("express");
const Holiday = require("../models/Holiday");

const router = express.Router();

const normalizeDateOnly = (value) => {
  if (!value) {
    return null;
  }

  const date = new Date(value);

  if (Number.isNaN(date.valueOf())) {
    return null;
  }

  date.setUTCHours(0, 0, 0, 0);
  return date;
};

router.get("/api/holidays", async (_req, res) => {
  try {
    const holidays = await Holiday.find().sort({ holiday: 1 });
    return res.json(holidays);
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch holidays" });
  }
});

router.post("/api/holidays", async (req, res) => {
  try {
    const { holiday, reason, isRecurring = false } = req.body || {};
    const normalizedDate = normalizeDateOnly(holiday);

    if (!normalizedDate) {
      return res.status(400).json({ message: "A valid holiday date is required" });
    }

    const trimmedReason =
      typeof reason === "string" ? reason.trim() : "";
    const isRecurringBool = Boolean(isRecurring);

    if (isRecurringBool) {
      const month = String(normalizedDate.getUTCMonth() + 1).padStart(2, "0");
      const day = String(normalizedDate.getUTCDate()).padStart(2, "0");
      const recurringKey = `${month}-${day}`;
      const existingRecurring = await Holiday.findOne({ recurringKey });

      if (existingRecurring) {
        return res
          .status(409)
          .json({ message: "A recurring holiday already exists for this date" });
      }
    } else {
      const startOfDay = new Date(normalizedDate);
      const endOfDay = new Date(normalizedDate);
      endOfDay.setUTCHours(23, 59, 59, 999);

      const existingExact = await Holiday.findOne({
        isRecurring: false,
        holiday: { $gte: startOfDay, $lte: endOfDay },
      });

      if (existingExact) {
        return res
          .status(409)
          .json({ message: "A holiday already exists on this date" });
      }
    }

    const created = await Holiday.create({
      holiday: normalizedDate,
      reason: trimmedReason,
      isRecurring: isRecurringBool,
    });

    return res.status(201).json(created);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to create holiday" });
  }
});

router.delete("/api/holidays/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const deleted = await Holiday.findByIdAndDelete(id);

    if (!deleted) {
      return res.status(404).json({ message: "Holiday not found" });
    }

    return res.json({ message: "Holiday deleted" });
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to delete holiday" });
  }
});

module.exports = router;
