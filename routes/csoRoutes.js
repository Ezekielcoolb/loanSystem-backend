const express = require("express");
const jwt = require("jsonwebtoken");
const mongoose = require("mongoose");
const CSO = require("../models/cso");
const Cso = require("../models/cso");
const Loan = require("../models/loan");
const Holiday = require("../models/Holiday");
const GroupLeader = require("../models/groupLeader");
const authenticateCso = require("../middleware/authenticateCso");
const jwtSecret = require("../config/jwtSecret");

const router = express.Router();

const FORM_AMOUNT_DEFAULT = 2000;

function generateToken(cso) {
  return jwt.sign({ id: cso._id, email: cso.email }, jwtSecret, {
    expiresIn: "7d",
  });
}

function parseNumeric(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

router.patch("/api/csos/defaulting-target", async (req, res) => {
  try {
    const scope = req.body.scope === "all" ? "all" : "single";
    const defaultingTarget = parseNumeric(req.body.defaultingTarget);

    if (!Number.isFinite(defaultingTarget) || defaultingTarget < 0) {
      return res
        .status(400)
        .json({ message: "Provide a valid non-negative number" });
    }

    const roundedTarget = Math.round(defaultingTarget);

    if (scope === "all") {
      const { modifiedCount } = await CSO.updateMany(
        {},
        { $set: { defaultingTarget: roundedTarget } }
      );
      return res.json({
        scope,
        data: { updatedCount: modifiedCount, defaultingTarget: roundedTarget },
      });
    }

    const { csoId } = req.body;

    if (!csoId || !mongoose.Types.ObjectId.isValid(csoId)) {
      return res
        .status(400)
        .json({ message: "Provide a valid CSO identifier" });
    }

    const updatedCso = await CSO.findByIdAndUpdate(
      csoId,
      { $set: { defaultingTarget: roundedTarget } },
      { new: true, runValidators: true }
    );

    if (!updatedCso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    return res.json({ scope, data: updatedCso });
  } catch (error) {
    return res.status(400).json({
      message: error.message || "Unable to update defaulting targets",
    });
  }
});

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
router.get("/api/csos", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const limit = Math.min(
      100,
      Math.max(1, parseInt(req.query.limit, 10) || 20)
    );
    const skip = (page - 1) * limit;

    const { branchId } = req.query;
    const query = branchId ? { branchId } : {};

    const [csos, total] = await Promise.all([
      CSO.find(query).sort({ createdAt: -1 }).skip(skip).limit(limit),
      CSO.countDocuments(query),
    ]);

    return res.json({
      data: csos,
      pagination: {
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit),
      },
    });
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

function getWeekdaysBetweenSync(startDate, endDate, holidaySet) {
  const normalizedStart = normalizeDate(startDate);
  const normalizedEnd = normalizeDate(endDate);

  if (!normalizedStart || !normalizedEnd || normalizedEnd < normalizedStart) {
    return 0;
  }

  const cursor = new Date(normalizedStart);
  let count = 0;

  while (cursor <= normalizedEnd) {
    const currentTime = normalizeDate(cursor).getTime();
    const day = cursor.getUTCDay();

    if (day !== 0 && day !== 6 && !holidaySet.has(currentTime)) {
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

function toCurrencyNumber(value) {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return 0;
  }

  return Number(number.toFixed(2));
}

function normalizeLocalDate(value) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return null;
  }

  date.setHours(0, 0, 0, 0);
  return date;
}

function addDaysLocal(date, amount) {
  const normalized = normalizeLocalDate(date);

  if (!normalized || !Number.isFinite(amount)) {
    return normalized;
  }

  const result = new Date(normalized);
  result.setDate(result.getDate() + amount);
  result.setHours(0, 0, 0, 0);
  return result;
}

function remittanceFallsWithinRange(record, startDate, endDate) {
  if (!record) {
    return false;
  }

  const primaryCandidate =
    record.date || record.updatedAt || record.createdAt || null;

  if (!primaryCandidate) {
    return false;
  }

  const normalized = normalizeLocalDate(primaryCandidate);

  if (!normalized) {
    return false;
  }

  const candidateTime = normalized.getTime();
  const startTime = startDate.getTime();
  const endTime = endDate.getTime();

  return candidateTime >= startTime && candidateTime < endTime;
}

function formatRemittanceEntry(csoDoc, remittanceDoc) {
  if (!remittanceDoc) {
    return null;
  }

  const csoData = csoDoc?.toObject ? csoDoc.toObject() : csoDoc;
  const record = remittanceDoc?.toObject
    ? remittanceDoc.toObject()
    : remittanceDoc;

  const amountCollected = toCurrencyNumber(record.amountCollected);
  const amountPaid = toCurrencyNumber(record.amountPaid);
  const amountRemitted = toCurrencyNumber(record.amountRemitted);
  const amountOnTeller = toCurrencyNumber(record.amountOnTeller);
  const difference = toCurrencyNumber(amountRemitted - amountOnTeller);
  const diffAbs = Math.abs(difference);

  const resolvedIssue = record.resolvedIssue || "";
  const issueResolution = record.issueResolution || "";

  let status = "balanced";
  if (diffAbs <= 0.5) {
    status = resolvedIssue ? "resolved" : "balanced";
  } else {
    status = resolvedIssue ? "resolved" : "issue";
  }

  const submittedDate = record.date ? new Date(record.date) : null;
  const createdAt = record.createdAt
    ? new Date(record.createdAt)
    : submittedDate;
  const updatedAt = record.updatedAt ? new Date(record.updatedAt) : createdAt;
  const submittedAt = createdAt || submittedDate;

  const csoNameParts = [csoData?.firstName, csoData?.lastName].filter(Boolean);
  const csoName =
    csoNameParts.length > 0
      ? csoNameParts.join(" ")
      : csoData?.email || csoData?.workId || "CSO";

  const dateToIso = (value) =>
    value && !Number.isNaN(value.valueOf()) ? value.toISOString() : null;

  return {
    id: String(record._id),
    csoId: String(csoData?._id || ""),
    csoName,
    branch: csoData?.branch || "",
    branchId: csoData?.branchId ? String(csoData.branchId) : "",
    amountCollected,
    amountPaid,
    amountRemitted,
    amountOnTeller,
    difference,
    issueResolution,
    resolvedIssue,
    remark: record.remark || "",
    image: record.image || "",
    date: dateToIso(submittedDate),
    submittedAt: dateToIso(submittedAt),
    createdAt: dateToIso(createdAt),
    updatedAt: dateToIso(updatedAt),
    hasIssue: diffAbs > 0.5 && !resolvedIssue,
    status,
  };
}

router.get("/api/admin/remittances", async (req, res) => {
  try {
    const now = new Date();
    const queryYear = Number.parseInt(req.query.year, 10);
    const queryMonth = Number.parseInt(req.query.month, 10);
    const csoId = req.query.csoId;
    const rangeParam = (req.query.range || "").toLowerCase();
    const dateParam = req.query.date;
    const fromParam = req.query.from;
    const toParam = req.query.to;

    const fallbackYear = Number.isFinite(queryYear)
      ? queryYear
      : now.getUTCFullYear();
    const fallbackMonthIndex = Number.isFinite(queryMonth)
      ? Math.min(Math.max(queryMonth - 1, 0), 11)
      : now.getUTCMonth();

    const today = normalizeLocalDate(new Date());
    let startDate = null;
    let endDate = null;

    const parsedDateParam = normalizeLocalDate(dateParam);
    const parsedFromParam = normalizeLocalDate(fromParam);
    const parsedToParam = normalizeLocalDate(toParam);

    if (rangeParam === "today") {
      startDate = today;
      endDate = addDaysLocal(today, 1);
    } else if (rangeParam === "yesterday") {
      endDate = today;
      startDate = addDaysLocal(endDate, -1);
    } else if (rangeParam === "week" || rangeParam === "thisweek") {
      endDate = addDaysLocal(today, 1);
      startDate = addDaysLocal(endDate, -7);
    } else if (parsedDateParam) {
      startDate = parsedDateParam;
      endDate = addDaysLocal(parsedDateParam, 1);
    }

    if (parsedFromParam) {
      startDate = parsedFromParam;
      if (!parsedToParam) {
        endDate = addDaysLocal(parsedFromParam, 1);
      }
    }

    if (parsedToParam) {
      endDate = addDaysLocal(parsedToParam, 1);
      if (!startDate) {
        startDate = addDaysLocal(endDate, -1);
      }
    }

    if (!startDate || !endDate) {
      startDate = normalizeLocalDate(
        new Date(fallbackYear, fallbackMonthIndex, 1)
      );
      endDate = normalizeLocalDate(
        new Date(fallbackYear, fallbackMonthIndex + 1, 1)
      );
    }

    if (startDate >= endDate) {
      endDate = addDaysLocal(startDate, 1);
    }

    const effectiveRange =
      rangeParam ||
      (parsedFromParam || parsedToParam || parsedDateParam
        ? "custom"
        : "month");

    const pageParam = Number.parseInt(req.query.page, 10);
    const limitParam = Number.parseInt(req.query.limit, 10);
    const page = Math.max(1, Number.isFinite(pageParam) ? pageParam : 1);
    const limit = Math.min(
      100,
      Math.max(5, Number.isFinite(limitParam) ? limitParam : 20)
    );

    const csoFilter = csoId ? { _id: csoId } : {};

    const csos = await CSO.find(csoFilter).select(
      "firstName lastName email workId branch branchId remittance"
    );

    const entries = [];

    for (const cso of csos) {
      if (!Array.isArray(cso?.remittance) || cso.remittance.length === 0) {
        continue;
      }

      for (const record of cso.remittance) {
        if (!remittanceFallsWithinRange(record, startDate, endDate)) {
          continue;
        }

        const formatted = formatRemittanceEntry(cso, record);
        if (formatted) {
          entries.push(formatted);
        }
      }
    }

    entries.sort((first, second) => {
      const firstTime = first?.submittedAt
        ? new Date(first.submittedAt).getTime()
        : first?.createdAt
        ? new Date(first.createdAt).getTime()
        : 0;
      const secondTime = second?.submittedAt
        ? new Date(second.submittedAt).getTime()
        : second?.createdAt
        ? new Date(second.createdAt).getTime()
        : 0;
      return secondTime - firstTime;
    });

    const total = entries.length;
    const totalPages = total > 0 ? Math.ceil(total / limit) : 1;
    const safePage = Math.min(page, totalPages);
    const sliceStart = total === 0 ? 0 : (safePage - 1) * limit;
    const sliceEnd = total === 0 ? 0 : sliceStart + limit;
    const paginated = total === 0 ? [] : entries.slice(sliceStart, sliceEnd);

    const summaryAccumulator = entries.reduce(
      (acc, entry) => {
        if (entry.status === "issue" || entry.hasIssue) {
          acc.unresolvedIssues += 1;
        } else if (entry.status === "resolved") {
          acc.resolvedCount += 1;
        } else {
          acc.balancedCount += 1;
        }
        return acc;
      },
      { unresolvedIssues: 0, resolvedCount: 0, balancedCount: 0 }
    );

    return res.json({
      data: paginated,
      meta: {
        page: safePage,
        limit,
        total,
        totalPages,
        year: fallbackYear,
        month: fallbackMonthIndex + 1,
        range: effectiveRange,
        filter: {
          startDate: startDate.toISOString().slice(0, 10),
          endDate: addDaysLocal(endDate, -1).toISOString().slice(0, 10),
          csoId: csoId || "",
        },
        summary: {
          totalRemittances: total,
          unresolvedIssues: summaryAccumulator.unresolvedIssues,
          resolvedCount: summaryAccumulator.resolvedCount,
          balancedCount: summaryAccumulator.balancedCount,
        },
        updatedAt: new Date().toISOString(),
      },
    });
  } catch (error) {
    console.error("Error fetching admin remittances:", error);
    return res
      .status(500)
      .json({ message: error.message || "Unable to load remittances" });
  }
});

router.patch("/api/admin/remittances/:remittanceId", async (req, res) => {
  try {
    const { remittanceId } = req.params;

    if (!remittanceId) {
      return res.status(400).json({ message: "Remittance ID is required" });
    }

    const {
      amountRemitted,
      amountOnTeller,
      issueResolution,
      resolvedNote,
      markResolved,
      clearResolved,
    } = req.body || {};

    const cso = await CSO.findOne({ "remittance._id": remittanceId });

    if (!cso) {
      return res.status(404).json({ message: "Remittance record not found" });
    }

    const remittanceRecord = cso.remittance.id(remittanceId);

    if (!remittanceRecord) {
      return res.status(404).json({ message: "Remittance record not found" });
    }

    if (amountRemitted !== undefined) {
      remittanceRecord.amountRemitted = toCurrencyNumber(amountRemitted);
    }

    if (amountOnTeller !== undefined) {
      remittanceRecord.amountOnTeller = toCurrencyNumber(amountOnTeller);
    }

    if (issueResolution !== undefined) {
      remittanceRecord.issueResolution = issueResolution;
    }

    if (clearResolved) {
      remittanceRecord.resolvedIssue = "";
    }

    if (markResolved) {
      remittanceRecord.resolvedIssue = resolvedNote || "Resolved by Admin";
    } else if (resolvedNote !== undefined && !markResolved) {
      remittanceRecord.resolvedIssue = resolvedNote;
    }

    const variance = Math.abs(
      toCurrencyNumber(
        (remittanceRecord.amountRemitted || 0) -
          (remittanceRecord.amountOnTeller || 0)
      )
    );

    if (variance <= 0.5 && !remittanceRecord.resolvedIssue) {
      remittanceRecord.resolvedIssue = "Balanced";
    }

    remittanceRecord.updatedAt = new Date();

    await cso.save();

    const updatedRecord = cso.remittance.id(remittanceId);

    return res.json({
      data: formatRemittanceEntry(cso, updatedRecord),
    });
  } catch (error) {
    console.error("Error updating admin remittance:", error);
    return res
      .status(500)
      .json({ message: error.message || "Unable to update remittance" });
  }
});

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
          const amountToBePaid =
            normalizeAmount(loan?.loanDetails?.amountToBePaid) || 0;

          const expectedAmount =
            businessDays >= 22
              ? amountToBePaid
              : normalizeAmount(businessDays * dailyAmount) || 0;

          const rawDue =
            normalizeAmount(expectedAmount - amountPaidToDate) || 0;
          amountDue = rawDue > 0 ? rawDue : 0;
          collectionStatus = amountDue > 0.01 ? "defaulting" : "paid";
        }
      } else {
        collectionStatus = "not due yet";
      }

      records.push({
        loanMongoId: loan._id,
        loanId: loan.loanId,
        loanStatus: loan.status,
        customerName: [
          loan?.customerDetails?.firstName,
          loan?.customerDetails?.lastName,
        ]
          .filter(Boolean)
          .join(" ")
          .trim(),
        groupName: loan?.groupDetails?.groupName || "",
        leaderName: loan?.groupDetails?.leaderName || "",
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
      const loanAppForm =
        normalizeAmount(loan?.loanDetails?.loanAppForm) || FORM_AMOUNT_DEFAULT;

      return {
        loanId: loan.loanId,
        customerName,
        groupName: loan?.groupDetails?.groupName || "",
        leaderName: loan?.groupDetails?.leaderName || "",
        loanAppForm,
        disbursedAt: loan?.disbursedAt,
      };
    });

    const totalLoanAppForm =
      normalizeAmount(
        records.reduce((sum, record) => sum + (record.loanAppForm || 0), 0)
      ) || 0;

    return res.json({
      date: targetDate.toISOString().slice(0, 10),
      summary: {
        totalCustomers: records.length,
        totalLoanAppForm,
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

router.get("/api/csos/dashboard-stats", authenticateCso, async (req, res) => {
  try {
    const { timeframe = "today" } = req.query;
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());

    let startDate, endDate;

    switch (timeframe) {
      case "yesterday":
        startDate = new Date(today);
        startDate.setDate(today.getDate() - 1);
        endDate = new Date(today);
        break;
      case "week":
        startDate = new Date(today);
        startDate.setDate(today.getDate() - today.getDay()); // Start of week (Sunday)
        endDate = new Date(now);
        break;
      case "month":
        startDate = new Date(now.getFullYear(), now.getMonth(), 1);
        endDate = new Date(now);
        break;
      case "year":
        startDate = new Date(now.getFullYear(), 0, 1);
        endDate = new Date(now);
        break;
      case "today":
      default:
        startDate = today;
        endDate = new Date(now);
        break;
    }

    const cso = await CSO.findById(req.cso._id);
    if (!cso) return res.status(404).json({ message: "CSO not found" });

    const loans = await Loan.find({ csoId: req.cso._id }).select(
      "loanId status amountDisbursed disbursedAt loanDetails customerDetails groupDetails createdAt"
    );

    let loanCount = 0;
    let totalDisbursed = 0;
    let totalPayments = 0;
    let totalForms = 0;
    let totalPending = 0;
    let defaultingCount = 0;

    // For monthly bar chart (current year)
    const currentYear = now.getFullYear();
    const monthlyCounts = Array(12)
      .fill(0)
      .map((_, i) => ({
        month: new Date(0, i).toLocaleString("en", { month: "short" }),
        count: 0,
        amount: 0,
      }));

    for (const loan of loans) {
      const disbursedAt = loan.disbursedAt ? new Date(loan.disbursedAt) : null;
      const createdAt = new Date(loan.createdAt);

      // Disbursement stats
      if (disbursedAt && disbursedAt >= startDate && disbursedAt <= endDate) {
        loanCount++;
        totalDisbursed +=
          loan.loanDetails?.amountDisbursed ||
          loan.loanDetails?.amountApproved ||
          0;
        totalForms += loan.loanDetails?.loanAppForm || FORM_AMOUNT_DEFAULT;
      }

      // Monthly aggregation for chart
      if (disbursedAt && disbursedAt.getFullYear() === currentYear) {
        monthlyCounts[disbursedAt.getMonth()].count++;
        monthlyCounts[disbursedAt.getMonth()].amount +=
          loan.loanDetails?.amountDisbursed ||
          loan.loanDetails?.amountApproved ||
          0;
      }

      // Collection stats (payments made in timeframe)
      const payments = loan.loanDetails?.dailyPayment || [];
      for (const p of payments) {
        const pDate = new Date(p.date);
        if (pDate >= startDate && pDate <= endDate) {
          totalPayments += p.amount || 0;
        }
      }

      // Pending Amount calculation logic (as of now)
      if (loan.status === "active loan") {
        const dailyAmount = loan.loanDetails?.dailyAmount || 0;
        const loanToBePaid = loan.loanDetails?.amountToBePaid || 0;
        const paidSoFar = loan.loanDetails?.amountPaidSoFar || 0;

        if (disbursedAt && dailyAmount > 0) {
          const dueStartDate = addDays(disbursedAt, 2);
          const businessDays = countBusinessDays(dueStartDate, now); // Up to today

          const expectedAmount =
            businessDays >= 22 ? loanToBePaid : businessDays * dailyAmount;
          const rawDue = expectedAmount - paidSoFar;
          const amountDue = rawDue > 0 ? rawDue : 0;

          totalPending += amountDue;
          if (amountDue > 0.01) {
            defaultingCount++;
          }
        }
      }
    }

    // Remittance stats in timeframe
    const timeframeRemittances = (cso.remittance || []).filter((r) => {
      const rDate = new Date(r.date);
      return rDate >= startDate && rDate <= endDate;
    });

    const amountCollected = timeframeRemittances.reduce(
      (sum, r) => sum + (Number(r.amountCollected) || 0),
      0
    );
    const amountPaidRemittance = timeframeRemittances.reduce(
      (sum, r) => sum + (Number(r.amountPaid) || 0),
      0
    );

    return res.json({
      timeframe,
      metrics: {
        loanCount,
        totalDisbursed,
        totalCollection: totalPayments + totalForms,
        pendingAmount: totalPending,
        defaultingCount,
      },
      targets: {
        loanTarget: cso.loanTarget || 0,
        disbursementTarget: cso.disbursementTarget || 0,
        defaultingTarget: cso.defaultingTarget || 0,
      },
      remittance: {
        collected: amountCollected,
        paid: amountPaidRemittance,
      },
      monthlyLoanStats: monthlyCounts,
    });
  } catch (error) {
    console.error("Dashboard Stats Error:", error);
    return res.status(500).json({
      message: error.message || "Unable to calculate dashboard statistics",
    });
  }
});

router.get("/api/admin/csos/:csoId/dashboard-stats", async (req, res) => {
  try {
    const { timeframe = "today" } = req.query;
    const { csoId } = req.params;

    if (!mongoose.Types.ObjectId.isValid(csoId)) {
      return res.status(400).json({ message: "Invalid CSO identifier" });
    }

    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());

    let startDate;
    let endDate;

    switch (timeframe) {
      case "yesterday":
        startDate = new Date(today);
        startDate.setDate(today.getDate() - 1);
        endDate = new Date(today);
        break;
      case "week":
        startDate = new Date(today);
        startDate.setDate(today.getDate() - today.getDay());
        endDate = new Date(now);
        break;
      case "month":
        startDate = new Date(now.getFullYear(), now.getMonth(), 1);
        endDate = new Date(now);
        break;
      case "year":
        startDate = new Date(now.getFullYear(), 0, 1);
        endDate = new Date(now);
        break;
      case "today":
      default:
        startDate = today;
        endDate = new Date(now);
        break;
    }

    const cso = await CSO.findById(csoId);
    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    const loans = await Loan.find({ csoId }).select(
      "loanId status amountDisbursed disbursedAt loanDetails customerDetails groupDetails createdAt"
    );

    let loanCount = 0;
    let totalDisbursed = 0;
    let totalPayments = 0;
    let totalForms = 0;
    let totalPending = 0;
    let defaultingCount = 0;

    const currentYear = now.getFullYear();
    const monthlyCounts = Array(12)
      .fill(0)
      .map((_, index) => ({
        month: new Date(0, index).toLocaleString("en", { month: "short" }),
        count: 0,
        amount: 0,
      }));

    for (const loan of loans) {
      const disbursedAt = loan.disbursedAt ? new Date(loan.disbursedAt) : null;

      if (disbursedAt && disbursedAt >= startDate && disbursedAt <= endDate) {
        loanCount += 1;
        totalDisbursed +=
          loan.loanDetails?.amountDisbursed ||
          loan.loanDetails?.amountApproved ||
          0;
        totalForms += loan.loanDetails?.loanAppForm || FORM_AMOUNT_DEFAULT;
      }

      if (disbursedAt && disbursedAt.getFullYear() === currentYear) {
        monthlyCounts[disbursedAt.getMonth()].count += 1;
        monthlyCounts[disbursedAt.getMonth()].amount +=
          loan.loanDetails?.amountDisbursed ||
          loan.loanDetails?.amountApproved ||
          0;
      }

      const payments = loan.loanDetails?.dailyPayment || [];
      for (const payment of payments) {
        const paymentDate = new Date(payment.date);
        if (paymentDate >= startDate && paymentDate <= endDate) {
          totalPayments += payment.amount || 0;
        }
      }

      if (loan.status === "active loan") {
        const dailyAmount = loan.loanDetails?.dailyAmount || 0;
        const loanToBePaid = loan.loanDetails?.amountToBePaid || 0;
        const paidSoFar = loan.loanDetails?.amountPaidSoFar || 0;

        if (disbursedAt && dailyAmount > 0) {
          const dueStartDate = addDays(disbursedAt, 2);
          const businessDays = countBusinessDays(dueStartDate, now);

          const expectedAmount =
            businessDays >= 22
              ? loanToBePaid
              : businessDays * dailyAmount;
          const rawDue = expectedAmount - paidSoFar;
          const amountDue = rawDue > 0 ? rawDue : 0;

          totalPending += amountDue;
          if (amountDue > 0.01) {
            defaultingCount += 1;
          }
        }
      }
    }

    const timeframeRemittances = (cso.remittance || []).filter((entry) => {
      const remittanceDate = new Date(entry.date);
      return remittanceDate >= startDate && remittanceDate <= endDate;
    });

    const amountCollected = timeframeRemittances.reduce(
      (sum, entry) => sum + (Number(entry.amountCollected) || 0),
      0
    );
    const amountPaidRemittance = timeframeRemittances.reduce(
      (sum, entry) => sum + (Number(entry.amountPaid) || 0),
      0
    );

    return res.json({
      timeframe,
      metrics: {
        loanCount,
        totalDisbursed,
        totalCollection: totalPayments + totalForms,
        pendingAmount: totalPending,
        defaultingCount,
      },
      targets: {
        loanTarget: cso.loanTarget || 0,
        disbursementTarget: cso.disbursementTarget || 0,
        defaultingTarget: cso.defaultingTarget || 0,
      },
      remittance: {
        collected: amountCollected,
        paid: amountPaidRemittance,
      },
      monthlyLoanStats: monthlyCounts,
    });
  } catch (error) {
    console.error("Admin CSO dashboard error:", error);
    return res.status(500).json({
      message: error.message || "Unable to load CSO dashboard",
    });
  }
});

router.get("/api/admin/csos/:csoId/outstanding-loans", async (req, res) => {
  try {
    const { csoId } = req.params;

    if (!mongoose.Types.ObjectId.isValid(csoId)) {
      return res.status(400).json({ message: "Invalid CSO identifier" });
    }

    const loans = await Loan.find({
      csoId,
      status: "active loan",
      disbursedAt: { $exists: true },
    });

    const today = new Date();
    today.setHours(0, 0, 0, 0);
    let normalizedSelectedDate = normalizeDate(today);

    const day = normalizedSelectedDate.getUTCDay();
    if (day === 6) {
      normalizedSelectedDate = addDays(normalizedSelectedDate, -1);
    } else if (day === 0) {
      normalizedSelectedDate = addDays(normalizedSelectedDate, -2);
    }

    const holidays = await Holiday.find({});
    const holidaySet = new Set(
      holidays
        .map((holiday) => normalizeDate(holiday.holiday))
        .filter(Boolean)
        .map((holidayDate) => holidayDate.getTime())
    );

    const results = loans.map((loan) => {
      const normalizedDisbursedAt = normalizeDate(loan.disbursedAt);
      const dailyAmount = loan.loanDetails?.dailyAmount || 0;
      const amountPaid = loan.loanDetails?.amountPaidSoFar || 0;
      const amountToBePaid = loan.loanDetails?.amountToBePaid || 0;
      const repaymentSchedule = loan.repaymentSchedule || [];

      const scheduleCountTillToday = repaymentSchedule.filter((entry) => {
        const entryDate = normalizeDate(entry.date);
        return entryDate && entryDate <= normalizedSelectedDate;
      }).length;

      let outstanding = 0;
      let expectedRepayment = 0;

      if (scheduleCountTillToday > 22) {
        expectedRepayment = amountToBePaid;
        outstanding = Math.max(0, amountToBePaid - amountPaid);
      } else {
        const daysElapsed = getWeekdaysBetweenSync(
          addDays(normalizedDisbursedAt, 1),
          normalizedSelectedDate,
          holidaySet
        );
        const effectiveDaysElapsed = Math.max(0, daysElapsed);
        const scheduleDaysDue = Math.max(0, scheduleCountTillToday - 1);
        const daysCounted = Math.min(effectiveDaysElapsed, scheduleDaysDue);
        expectedRepayment = dailyAmount * daysCounted;
        outstanding = Math.max(0, expectedRepayment - amountPaid);
      }

      return {
        ...loan.toObject(),
        metrics: {
          amountPaid,
          dailyAmount,
          expectedRepayment,
          outstandingDue: outstanding,
        },
      };
    });

    const filtered = results.filter(
      (loan) => loan.metrics.outstandingDue > 0.01
    );
    const totalOutstanding = filtered.reduce(
      (sum, loan) => sum + loan.metrics.outstandingDue,
      0
    );

    return res.json({ loans: filtered, totalOutstanding });
  } catch (error) {
    console.error("Admin CSO outstanding error:", error);
    return res.status(500).json({
      message: error.message || "Unable to load CSO outstanding loans",
    });
  }
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

// Transfer CSO to a new branch
router.patch("/api/csos/:id/transfer-branch", async (req, res) => {
  try {
    const { id } = req.params;
    const { branch, branchId } = req.body;

    if (!branch || !branchId) {
      return res
        .status(400)
        .json({ message: "Branch name and ID are required" });
    }

    const cso = await CSO.findByIdAndUpdate(
      id,
      { $set: { branch, branchId } },
      { new: true, runValidators: true }
    );

    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    // Update all loans associated with this CSO
    await Loan.updateMany(
      { csoId: id },
      {
        $set: {
          branch: branch,
          branchId: branchId,
        },
      }
    );

    return res.json({
      message: "CSO and all associated loans transferred successfully",
      cso,
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to transfer CSO branch" });
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

    const now = new Date();

    const remittanceData = {
      date: targetDate,
      amountCollected: Number(amountCollected),
      amountPaid: Number(amountPaid),
      amountRemitted: toCurrencyNumber(amountPaid),
      amountOnTeller: 0,
      image: image || "",
      remark: remark || "",
      issueResolution: "",
      resolvedIssue: resolvedIssue || "",
      createdAt: now,
      updatedAt: now,
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

    const totalLoanAppForm = disbursedLoans.reduce((sum, loan) => {
      const loanAppForm =
        normalizeAmount(loan?.loanDetails?.loanAppForm) || FORM_AMOUNT_DEFAULT; // Default if missing
      return sum + loanAppForm;
    }, 0);

    const totalCollection = normalizeAmount(totalPaidToday + totalLoanAppForm);

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

// Create a new group leader
router.post("/api/group-leaders", authenticateCso, async (req, res) => {
  try {
    const { groupName, firstName, lastName, address, phone } = req.body;

    if (!groupName || !firstName || !lastName || !address || !phone) {
      return res.status(400).json({ message: "All fields are required" });
    }

    const cso = await CSO.findById(req.cso._id);
    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    const groupLeader = await GroupLeader.create({
      groupName,
      firstName,
      lastName,
      address,
      phone,
      csoId: req.cso._id,
      csoName: `${cso.firstName} ${cso.lastName}`,
    });

    res.status(201).json(groupLeader);
  } catch (error) {
    if (error.code === 11000) {
      return res.status(409).json({ message: "Phone number already exists" });
    }
    return res
      .status(400)
      .json({ message: error.message || "Unable to create group leader" });
  }
});

// Get all group leaders (for admin)
router.get("/api/group-leaders", async (req, res) => {
  try {
    const { csoId } = req.query;
    const query = csoId ? { csoId } : {};
    const groupLeaders = await GroupLeader.find(query).sort({ createdAt: -1 });
    res.json(groupLeaders);
  } catch (error) {
    res
      .status(400)
      .json({ message: error.message || "Unable to fetch group leaders" });
  }
});

// Approve group leader
router.put("/api/group-leaders/:id/approve", async (req, res) => {
  try {
    const { id } = req.params;

    const groupLeader = await GroupLeader.findByIdAndUpdate(
      id,
      { status: "approved" },
      { new: true, runValidators: true }
    );

    if (!groupLeader) {
      return res.status(404).json({ message: "Group leader not found" });
    }

    res.json(groupLeader);
  } catch (error) {
    res
      .status(400)
      .json({ message: error.message || "Unable to approve group leader" });
  }
});

// Update group leader
router.put("/api/group-leaders/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { groupName, firstName, lastName, address, phone } = req.body;

    const groupLeader = await GroupLeader.findByIdAndUpdate(
      id,
      { groupName, firstName, lastName, address, phone },
      { new: true, runValidators: true }
    );

    if (!groupLeader) {
      return res.status(404).json({ message: "Group leader not found" });
    }

    res.json(groupLeader);
  } catch (error) {
    if (error.code === 11000) {
      return res.status(409).json({ message: "Phone number already exists" });
    }
    res
      .status(400)
      .json({ message: error.message || "Unable to update group leader" });
  }
});

// Delete group leader
router.delete("/api/group-leaders/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const groupLeader = await GroupLeader.findByIdAndDelete(id);

    if (!groupLeader) {
      return res.status(404).json({ message: "Group leader not found" });
    }

    res.json({ message: "Group leader deleted successfully" });
  } catch (error) {
    res
      .status(400)
      .json({ message: error.message || "Unable to delete group leader" });
  }
});

// Transfer group leader to a new CSO
router.post("/api/group-leaders/:id/transfer-cso", async (req, res) => {
  try {
    const { id } = req.params;
    const { newCsoId } = req.body;

    if (!newCsoId) {
      return res.status(400).json({ message: "New CSO ID is required" });
    }

    // Fetch the group leader
    const groupLeader = await GroupLeader.findById(id);
    if (!groupLeader) {
      return res.status(404).json({ message: "Group leader not found" });
    }

    // Fetch the new CSO details
    const newCso = await CSO.findById(newCsoId);
    if (!newCso) {
      return res.status(404).json({ message: "New CSO not found" });
    }

    // Update the group leader
    groupLeader.csoId = newCsoId;
    groupLeader.csoName = `${newCso.firstName} ${newCso.lastName}`;
    await groupLeader.save();

    // Update all loans under this group
    const Loan = require("../models/loan");
    const updateResult = await Loan.updateMany(
      { "groupDetails.groupId": id },
      {
        $set: {
          csoId: newCsoId,
          csoName: `${newCso.firstName} ${newCso.lastName}`,
          csoSignature: newCso.signature || "",
          branch: newCso.branch || "",
          branchId: newCso.branchId || "",
        },
      }
    );

    res.json({
      message: "Group transferred successfully",
      groupLeader,
      loansTransferred: updateResult.modifiedCount,
    });
  } catch (error) {
    console.error("Error transferring group:", error);
    res
      .status(500)
      .json({ message: error.message || "Unable to transfer group" });
  }
});

// Get group leaders for a specific CSO (admin access)
router.get("/api/csos/:id/group-leaders", async (req, res) => {
  try {
    const { id } = req.params;
    const groupLeaders = await GroupLeader.find({
      csoId: id,
      status: "approved",
    }).sort({ groupName: 1 });
    res.json(groupLeaders);
  } catch (error) {
    res
      .status(400)
      .json({ message: error.message || "Unable to fetch group leaders" });
  }
});

router.get(
  "/api/group-leaders/my-approved",
  authenticateCso,
  async (req, res) => {
    try {
      const groupLeaders = await GroupLeader.find({
        csoId: req.cso._id,
        status: "approved",
      }).sort({ createdAt: -1 });

      return res.json(groupLeaders);
    } catch (error) {
      return res
        .status(400)
        .json({ message: error.message || "Unable to fetch group leaders" });
    }
  }
);

module.exports = router;
