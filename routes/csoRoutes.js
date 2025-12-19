const express = require("express");
const jwt = require("jsonwebtoken");
const CSO = require("../models/cso");
const Cso = require("../models/cso");
const Loan = require("../models/loan");
const authenticateCso = require("../middleware/authenticateCso");

function generateToken(cso) {
  return jwt.sign({ id: cso._id, email: cso.email }, process.env.JWT_SECRET, {
    expiresIn: "7d",
  });
}

const router = express.Router();

// Create a new CSO
router.post("/api/csos", async (req, res) => {
  try {
    const payload = { ...req.body };

    if (!payload.password && payload.workId) {
      payload.password = payload.workId;
    }

    const cso = await CSO.create(payload);
    return res.status(201).json(cso);
  } catch (error) {
    if (error.code === 11000) {
      return res.status(409).json({ message: "CSO email already exists" });
    }
    return res
      .status(400)
      .json({ message: error.message || "Unable to create CSO" });
  }
});

// CSO login
router.post("/api/csos/login", async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res
        .status(400)
        .json({ message: "Email and password are required" });
    }

    const cso = await CSO.findOne({ email }).select("+password");

    if (!cso) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const isMatch = await cso.comparePassword(password);

    if (!isMatch) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const token = generateToken(cso);
    return res.json({ token, cso: cso.toJSON() });
  } catch (error) {
    return res.status(500).json({ message: "Unable to login" });
  }
});

// Reset CSO password without current password (forgot password flow)
router.post("/api/csos/forgot-password", async (req, res) => {
  try {
    const { email, newPassword } = req.body;

    if (!email || !newPassword) {
      return res
        .status(400)
        .json({ message: "Email and new password are required" });
    }

    if (typeof newPassword !== "string" || newPassword.length < 8) {
      return res
        .status(400)
        .json({ message: "New password must be at least 8 characters" });
    }

    const cso = await CSO.findOne({ email }).select("+password");

    if (!cso) {
      return res
        .status(404)
        .json({ message: "CSO with provided email not found" });
    }

    cso.password = newPassword;
    await cso.save();

    return res.json({ message: "Password reset successfully" });
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to reset password" });
  }
});

// Retrieve all CSOs
router.get("/api/csos", async (_req, res) => {
  try {
    const csos = await CSO.find().sort({ createdAt: -1 });
    return res.json(csos);
  } catch (error) {
    return res.status(500).json({ message: "Unable to fetch CSOs" });
  }
});

function normalizeDate(value) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return null;
  }

  date.setUTCHours(0, 0, 0, 0);
  return date;
}

function isWeekend(date) {
  const day = date.getUTCDay();
  return day === 6 || day === 0;
}

function addDays(date, amount) {
  const normalized = normalizeDate(date);

  if (!normalized || !Number.isFinite(amount)) {
    return normalized;
  }

  const result = new Date(normalized);
  result.setUTCDate(result.getUTCDate() + amount);
  return result;
}

function formatDateKey(value) {
  const normalized = normalizeDate(value);

  if (!normalized) {
    return null;
  }

  return normalized.toISOString().slice(0, 10);
}

function datesAreSameDay(first, second) {
  const firstKey = formatDateKey(first);
  const secondKey = formatDateKey(second);

  return Boolean(firstKey && secondKey && firstKey === secondKey);
}

function countBusinessDays(startDate, endDate) {
  const normalizedStart = normalizeDate(startDate);
  const normalizedEnd = normalizeDate(endDate);

  if (!normalizedStart || !normalizedEnd || normalizedEnd < normalizedStart) {
    return 0;
  }

  const cursor = new Date(normalizedStart);
  let count = 0;

  while (cursor <= normalizedEnd) {
    if (!isWeekend(cursor)) {
      count += 1;
    }

    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  return count;
}

function normalizeAmount(value) {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return null;
  }

  return Number(number.toFixed(2));
}

// CSO daily collection summary
router.get("/api/csos/collection", authenticateCso, async (req, res) => {
  try {
    const targetDate = normalizeDate(req.query.date || new Date());

    if (!targetDate) {
      return res.status(400).json({ message: "Invalid date supplied" });
    }

    const loans = await Loan.find({
      csoId: req.cso._id,
      status: { $in: ["active loan", "fully paid"] },
    });

    const records = [];

    for (const loan of loans) {
      const dailyAmount = normalizeAmount(loan?.loanDetails?.dailyAmount) || 0;
      const disbursedDate = normalizeDate(loan?.disbursedAt);
      const payments = Array.isArray(loan?.loanDetails?.dailyPayment)
        ? loan.loanDetails.dailyPayment
        : [];

      const sanitizedPayments = payments
        .map((payment) => ({
          amount: normalizeAmount(payment?.amount),
          date: normalizeDate(payment?.date),
        }))
        .filter(
          (payment) => payment.amount && payment.amount > 0 && payment.date
        );

      const amountPaidToday = sanitizedPayments
        .filter((payment) => datesAreSameDay(payment.date, targetDate))
        .reduce((sum, payment) => sum + payment.amount, 0);

      const amountPaidToDate = sanitizedPayments
        .filter((payment) => payment.date <= targetDate)
        .reduce((sum, payment) => sum + payment.amount, 0);

      if (loan.status === "fully paid" && amountPaidToday <= 0) {
        continue;
      }

      let amountDue = 0;
      let collectionStatus = "paid";

      if (disbursedDate && dailyAmount > 0) {
        const dueStartDate = addDays(disbursedDate, 2);

        if (targetDate < dueStartDate) {
          collectionStatus = "not due yet";
        } else {
          const businessDays = countBusinessDays(dueStartDate, targetDate);
          const expectedAmount =
            normalizeAmount(businessDays * dailyAmount) || 0;
          const rawDue =
            normalizeAmount(expectedAmount - amountPaidToDate) || 0;
          amountDue = rawDue > 0 ? rawDue : 0;
          collectionStatus = amountDue > 0.01 ? "defaulting" : "paid";
        }
      } else {
        collectionStatus = "not due yet";
      }

      records.push({
        loanId: loan.loanId,
        loanStatus: loan.status,
        customerName: [
          loan?.customerDetails?.firstName,
          loan?.customerDetails?.lastName,
        ]
          .filter(Boolean)
          .join(" ")
          .trim(),
        amountPaidToday: normalizeAmount(amountPaidToday) || 0,
        amountPaidToDate: normalizeAmount(amountPaidToDate) || 0,
        amountDue,
        dailyAmount,
        disbursedAt: loan?.disbursedAt,
        collectionStatus,
      });
    }

    const summary = records.reduce(
      (acc, record) => {
        acc.totalCustomers += 1;
        acc.totalPaidToday =
          normalizeAmount(
            (acc.totalPaidToday || 0) + (record.amountPaidToday || 0)
          ) || 0;
        acc.totalDue =
          normalizeAmount((acc.totalDue || 0) + (record.amountDue || 0)) || 0;

        if (record.collectionStatus === "defaulting") {
          acc.defaultingCount += 1;
        }

        return acc;
      },
      { totalCustomers: 0, totalPaidToday: 0, totalDue: 0, defaultingCount: 0 }
    );

    return res.json({
      date: targetDate.toISOString().slice(0, 10),
      summary,
      records,
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to load collection" });
  }
});

router.get("/api/csos/form-collection", authenticateCso, async (req, res) => {
  try {
    const targetDate = normalizeDate(req.query.date || new Date());

    if (!targetDate) {
      return res.status(400).json({ message: "Invalid date supplied" });
    }

    const nextDate = addDays(targetDate, 1);

    if (!nextDate) {
      return res
        .status(400)
        .json({ message: "Unable to determine date range" });
    }

    const loans = await Loan.find({
      csoId: req.cso._id,
      status: { $in: ["active loan", "fully paid"] },
      disbursedAt: {
        $gte: targetDate,
        $lt: nextDate,
      },
    }).sort({ disbursedAt: 1 });

    const records = loans.map((loan) => {
      const customerName = [
        loan?.customerDetails?.firstName,
        loan?.customerDetails?.lastName,
      ]
        .filter(Boolean)
        .join(" ")
        .trim();
      const formAmount =
        normalizeAmount(loan?.loanDetails?.formAmount) || FORM_AMOUNT_DEFAULT;

      return {
        loanId: loan.loanId,
        customerName,
        formAmount,
        disbursedAt: loan?.disbursedAt,
      };
    });

    const totalFormAmount =
      normalizeAmount(
        records.reduce((sum, record) => sum + (record.formAmount || 0), 0)
      ) || 0;

    return res.json({
      date: targetDate.toISOString().slice(0, 10),
      summary: {
        totalCustomers: records.length,
        totalFormAmount,
      },
      records,
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to load form collection" });
  }
});

// Retrieve authenticated CSO profile
router.get("/api/csos/me", authenticateCso, (req, res) => {
  return res.json(req.cso.toJSON());
});

// Retrieve a single CSO by ID
router.get("/api/csos/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const cso = await CSO.findById(id);

    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    return res.json(cso);
  } catch (error) {
    return res.status(400).json({ message: "Unable to fetch CSO" });
  }
});

// Update CSO information
router.patch("/api/csos/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updates = { ...req.body };

    if (!updates.password && updates.workId) {
      updates.password = updates.workId;
    }

    const cso = await CSO.findByIdAndUpdate(id, updates, {
      new: true,
      runValidators: true,
    });

    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    return res.json(cso);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to update CSO" });
  }
});

// Activate or deactivate CSO
router.patch("/api/csos/:id/status", async (req, res) => {
  try {
    const { id } = req.params;
    const { isActive } = req.body;

    if (typeof isActive !== "boolean") {
      return res.status(400).json({ message: "isActive must be a boolean" });
    }

    const cso = await CSO.findByIdAndUpdate(
      id,
      { $set: { isActive } },
      { new: true, runValidators: true }
    );

    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    return res.json(cso);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to update CSO status" });
  }
});

// Update authenticated CSO profile details (phone, profile image)
router.patch("/api/csos/me/profile", authenticateCso, async (req, res) => {
  try {
    const updates = {};
    const { phone, profileImg } = req.body;

    if (typeof phone === "string" && phone.trim() !== "") {
      updates.phone = phone.trim();
    }

    if (typeof profileImg === "string") {
      updates.profileImg = profileImg.trim();
    }

    if (Object.keys(updates).length === 0) {
      return res
        .status(400)
        .json({ message: "No valid profile updates provided" });
    }

    const updated = await CSO.findByIdAndUpdate(
      req.cso._id,
      { $set: updates },
      {
        new: true,
        runValidators: true,
      }
    );

    return res.json(updated.toJSON());
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to update profile" });
  }
});

// Update authenticated CSO signature
router.patch("/api/csos/me/signature", authenticateCso, async (req, res) => {
  try {
    const { signature } = req.body;

    if (typeof signature !== "string") {
      return res.status(400).json({ message: "Signature must be provided" });
    }

    const updated = await CSO.findByIdAndUpdate(
      req.cso._id,
      { $set: { signature } },
      { new: true, runValidators: true }
    );

    return res.json(updated.toJSON());
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to update signature" });
  }
});

// Update authenticated CSO password
router.patch("/api/csos/me/password", authenticateCso, async (req, res) => {
  try {
    const { currentPassword, newPassword, confirmPassword } = req.body;

    if (!currentPassword || !newPassword || !confirmPassword) {
      return res
        .status(400)
        .json({ message: "All password fields are required" });
    }

    if (newPassword !== confirmPassword) {
      return res.status(400).json({ message: "New passwords do not match" });
    }

    if (newPassword.length < 8) {
      return res
        .status(400)
        .json({ message: "New password must be at least 8 characters" });
    }

    const cso = await CSO.findById(req.cso._id).select("+password");

    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    const isMatch = await cso.comparePassword(currentPassword);

    if (!isMatch) {
      return res.status(400).json({ message: "Current password is incorrect" });
    }

    cso.password = newPassword;
    await cso.save();

    return res.json({ message: "Password updated successfully" });
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to update password" });
  }
});

// Post daily remittance
router.post("/api/csos/remittance", authenticateCso, async (req, res) => {
  try {
    const { amountCollected, amountPaid, image, date, remark, resolvedIssue } =
      req.body;

    if (!amountCollected || !amountPaid || !date) {
      return res.status(400).json({
        message: "Amount collected, amount paid, and date are required",
      });
    }

    const targetDate = normalizeDate(date);
    if (!targetDate) {
      return res.status(400).json({ message: "Invalid date format" });
    }

    const cso = await CSO.findById(req.cso._id);
    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    const remittanceData = {
      date: targetDate,
      amountCollected: Number(amountCollected),
      amountPaid: Number(amountPaid),
      image: image || "",
      remark: remark || "",
      resolvedIssue: resolvedIssue || "",
    };

    // Always add a new remittance record to support multiple partial payments/images
    cso.remittance.push(remittanceData);

    await cso.save();
    return res.json({
      message: "Remittance posted successfully",
      remittance: cso.remittance,
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to post remittance" });
  }
});

// Resolve a remittance issue
router.post("/api/csos/:id/resolve-remittance", async (req, res) => {
  try {
    const { date, resolvedIssue } = req.body;
    const csoId = req.params.id;

    if (!date || !resolvedIssue) {
      return res
        .status(400)
        .json({ message: "Date and resolution message are required" });
    }

    const targetDate = normalizeDate(date);
    if (!targetDate) {
      return res.status(400).json({ message: "Invalid date format" });
    }

    const today = new Date();
    today.setHours(0, 0, 0, 0);
    if (targetDate > today) {
      return res
        .status(400)
        .json({ message: "Cannot resolve remittance for future dates" });
    }

    const targetDateStr = targetDate.toISOString().slice(0, 10);
    const nextDate = addDays(targetDate, 1);

    const cso = await CSO.findById(csoId);
    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    // Calculate Total Collection for the date
    // 1. Loan Payments
    const loans = await Loan.find({
      csoId: csoId,
      status: { $in: ["active loan", "fully paid"] },
    });

    let totalPaidToday = 0;
    for (const loan of loans) {
      const payments = Array.isArray(loan?.loanDetails?.dailyPayment)
        ? loan.loanDetails.dailyPayment
        : [];
      const amountForLoan = payments
        .filter((p) => datesAreSameDay(p.date, targetDate))
        .reduce((sum, p) => sum + (normalizeAmount(p.amount) || 0), 0);
      totalPaidToday += amountForLoan;
    }

    // 2. Form Collection (Loans disbursed on that date)
    const disbursedLoans = await Loan.find({
      csoId: csoId,
      status: { $in: ["active loan", "fully paid"] },
      disbursedAt: {
        $gte: targetDate,
        $lt: nextDate,
      },
    });

    const totalFormAmount = disbursedLoans.reduce((sum, loan) => {
      const formAmount = normalizeAmount(loan?.loanDetails?.formAmount) || 2000; // Default 2000 if missing
      return sum + formAmount;
    }, 0);

    const totalCollection = normalizeAmount(totalPaidToday + totalFormAmount);

    const existingRemittance = cso.remittance.find((r) => {
      const rDate = new Date(r.date).toISOString().slice(0, 10);
      return rDate === targetDateStr;
    });

    if (existingRemittance) {
      existingRemittance.resolvedIssue = resolvedIssue;
      existingRemittance.amountCollected = totalCollection; // Update collected amount
    } else {
      cso.remittance.push({
        date: targetDate,
        amountCollected: totalCollection,
        amountPaid: 0,
        image: "",
        remark: "Resolved by Admin",
        resolvedIssue: resolvedIssue,
      });
    }

    await cso.save();
    res.json(cso);
  } catch (error) {
    console.error("Error resolving remittance:", error);
    res.status(500).json({ message: "Server error resolving remittance" });
  }
});

module.exports = router;
