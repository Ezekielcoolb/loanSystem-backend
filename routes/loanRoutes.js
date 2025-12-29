const express = require("express");
const Loan = require("../models/loan");
const CSO = require("../models/cso");
const authenticateCso = require("../middleware/authenticateCso");

const router = express.Router();

const WORKING_DAYS_COUNT = 23;
const INSTALLMENT_DAYS_COUNT = 22;
const WEEKLY_INSTALLMENT_COUNT = 5;
const FORM_AMOUNT_DEFAULT = 2000;

function generateLoanId() {
  return `LN-${Date.now()}`;
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

    cursor.setDate(cursor.getDate() + 1);
  }

  return count;
}

function isWeekend(date) {
  const day = date.getDay();
  return day === 6 || day === 0;
}

function generateDailyRepaymentSchedule(startDate) {
  const schedule = [];
  let cursor = new Date(startDate);

  while (schedule.length < WORKING_DAYS_COUNT) {
    if (!isWeekend(cursor)) {
      schedule.push({
        date: new Date(cursor),
        status: schedule.length === 0 ? "approved" : "pending",
        amountPaid: 0,
      });
    }

    cursor = new Date(cursor);
    cursor.setDate(cursor.getDate() + 1);
  }

  return schedule;
}

function generateWeeklyRepaymentSchedule(startDate) {
  const schedule = [];
  let cursor = new Date(startDate);

  for (
    let installment = 0;
    installment < WEEKLY_INSTALLMENT_COUNT;
    installment += 1
  ) {
    schedule.push({
      date: new Date(cursor),
      status: installment === 0 ? "approved" : "pending",
      amountPaid: 0,
    });

    cursor = new Date(cursor);
    cursor.setDate(cursor.getDate() + 7);
  }

  return schedule;
}

function generateRepaymentSchedule(startDate, loanType) {
  if (loanType === "weekly") {
    return generateWeeklyRepaymentSchedule(startDate);
  }

  return generateDailyRepaymentSchedule(startDate);
}

function normalizeDate(value) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return null;
  }

  date.setHours(0, 0, 0, 0);
  return date;
}

function addDays(date, amount) {
  const normalized = normalizeDate(date);

  if (!normalized || !Number.isFinite(amount)) {
    return normalized;
  }

  const result = new Date(normalized);
  result.setDate(result.getDate() + amount);
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

function getNextBusinessDay(date) {
  let cursor = normalizeDate(date);

  if (!cursor) {
    return null;
  }

  do {
    cursor.setDate(cursor.getDate() + 1);
  } while (isWeekend(cursor));

  return cursor;
}

function ensureScheduleEntry(schedule, targetDate) {
  let normalized = normalizeDate(targetDate);

  if (!normalized) {
    return null;
  }

  if (isWeekend(normalized)) {
    normalized = getNextBusinessDay(normalized);
  }

  const existing = schedule.find((entry) =>
    datesAreSameDay(entry.date, normalized)
  );

  if (existing) {
    existing.date = normalizeDate(existing.date);
    existing.amountPaid = Number(existing.amountPaid || 0);
    return existing;
  }

  const newEntry = {
    date: normalized,
    status: "pending",
    amountPaid: 0,
  };

  schedule.push(newEntry);
  return newEntry;
}

function normalizeAmount(value) {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return null;
  }

  return Number(number.toFixed(2));
}

function finalizeEntryStatus(entry, index, dailyAmount) {
  const tolerance = 0.01;
  const paid = normalizeAmount(entry.amountPaid) || 0;

  if (entry.status === "holiday") {
    entry.amountPaid = paid;
    return;
  }

  if (paid >= dailyAmount - tolerance) {
    entry.amountPaid = dailyAmount;
    entry.status = "paid";
  } else if (paid > tolerance) {
    entry.amountPaid = paid;
    entry.status = "partial";
  } else {
    entry.amountPaid = 0;

    if (entry.status === "submitted") {
      entry.status = "submitted";
    } else if (index === 0 && entry.status === "approved") {
      entry.status = "approved";
    } else {
      entry.status = "pending";
    }
  }
}

function initializeRepaymentSchedule(loan, fallbackStartDate) {
  const hasExistingSchedule =
    Array.isArray(loan.repaymentSchedule) && loan.repaymentSchedule.length > 0;
  const startDate = normalizeDate(fallbackStartDate) || new Date();
  const baseEntries = hasExistingSchedule
    ? loan.repaymentSchedule
    : generateRepaymentSchedule(startDate, loan.loanDetails?.loanType);

  const schedule = [];
  const seenDates = new Set();

  baseEntries
    .map((entry, index) => {
      const normalizedDate = normalizeDate(entry?.date);
      const key = formatDateKey(normalizedDate);

      if (!normalizedDate || !key || seenDates.has(key)) {
        return null;
      }

      seenDates.add(key);

      let status = typeof entry?.status === "string" ? entry.status : "pending";

      if (status === "holiday") {
        // keep holiday as-is
      } else if (status === "submitted") {
        // keep submitted as-is
      } else if (status !== "approved") {
        status = index === 0 ? "approved" : "pending";
      }

      return {
        date: normalizedDate,
        status,
        amountPaid: 0,
        holidayReason: entry?.holidayReason,
      };
    })
    .filter(Boolean)
    .forEach((entry) => {
      schedule.push(entry);
    });

  schedule.sort((first, second) => first.date - second.date);

  if (schedule.length > 0) {
    const firstEntry = schedule[0];

    if (firstEntry.status !== "submitted" && firstEntry.status !== "holiday") {
      firstEntry.status = "approved";
    }
  }

  return schedule;
}

function syncLoanRepaymentSchedule(loan) {
  const dailyAmount = normalizeAmount(loan?.loanDetails?.dailyAmount);

  if (!dailyAmount || dailyAmount <= 0) {
    throw new Error("Loan is missing a valid dailyAmount");
  }

  const rawPayments = Array.isArray(loan?.loanDetails?.dailyPayment)
    ? loan.loanDetails.dailyPayment
    : [];
  const sanitizedPayments = rawPayments
    .map((payment) => ({
      amount: normalizeAmount(payment?.amount),
      date: normalizeDate(payment?.date),
    }))
    .filter((payment) => payment.amount && payment.amount > 0 && payment.date);

  sanitizedPayments.sort((first, second) => first.date - second.date);

  const fallbackStartDate =
    normalizeDate(loan?.disbursedAt) ||
    sanitizedPayments[0]?.date ||
    new Date();
  const schedule = initializeRepaymentSchedule(loan, fallbackStartDate);

  let totalApplied = 0;

  for (const payment of sanitizedPayments) {
    let remainingAmount = payment.amount;
    let cursorDate = payment.date;

    while (remainingAmount > 0.0001) {
      const entry = ensureScheduleEntry(schedule, cursorDate);

      if (!entry) {
        break;
      }

      if (entry.status === "holiday") {
        cursorDate = getNextBusinessDay(cursorDate);

        if (!cursorDate) {
          break;
        }

        continue;
      }

      const currentPaid = normalizeAmount(entry.amountPaid) || 0;
      const capacity = normalizeAmount(dailyAmount - currentPaid) || 0;

      if (capacity <= 0) {
        cursorDate = getNextBusinessDay(cursorDate);

        if (!cursorDate) {
          break;
        }

        continue;
      }

      const applied = normalizeAmount(Math.min(remainingAmount, capacity));

      if (!applied) {
        break;
      }

      entry.amountPaid = normalizeAmount(currentPaid + applied);
      remainingAmount = normalizeAmount(remainingAmount - applied) || 0;
      totalApplied = normalizeAmount(totalApplied + applied) || 0;

      if (remainingAmount > 0.0001) {
        cursorDate = getNextBusinessDay(cursorDate);

        if (!cursorDate) {
          break;
        }
      }
    }
  }

  schedule.sort((first, second) => first.date - second.date);
  schedule.forEach((entry, index) =>
    finalizeEntryStatus(entry, index, dailyAmount)
  );

  const amountPaidSoFar =
    normalizeAmount(
      schedule.reduce(
        (sum, entry) => sum + (normalizeAmount(entry.amountPaid) || 0),
        0
      )
    ) || 0;

  if (!loan.loanDetails) {
    loan.loanDetails = {};
  }

  loan.repaymentSchedule = schedule;
  loan.loanDetails.amountPaidSoFar = amountPaidSoFar;

  return {
    schedule,
    amountPaidSoFar,
    totalApplied,
  };
}

function buildLoanPayload(body, cso) {
  const {
    loanId,
    customerDetails,
    businessDetails,
    bankDetails,
    loanDetails = {},
    guarantorDetails,
    guarantorFormPic,
    groupDetails,
    pictures,
  } = body || {};

  if (
    !customerDetails ||
    !businessDetails ||
    !bankDetails ||
    !guarantorDetails
  ) {
    throw new Error("Missing required loan sections");
  }

  if (!loanDetails.amountRequested || !loanDetails.loanType) {
    throw new Error("Loan amount and type are required");
  }

  const derivedLoanDetails = {
    ...loanDetails,
    formAmount: FORM_AMOUNT_DEFAULT,
    dailyPayment: Array.isArray(loanDetails.dailyPayment)
      ? loanDetails.dailyPayment
      : [],
    amountPaidSoFar: loanDetails.amountPaidSoFar || 0,
    penalty: loanDetails.penalty || 0,
    penaltyPaid: loanDetails.penaltyPaid || 0,
  };

  return {
    csoId: cso._id,
    csoSignature: cso.signature || null,
    branch: cso.branch || "",
    branchId: cso.branchId || "",
    csoName: [cso.firstName, cso.lastName].filter(Boolean).join(" "),
    loanId: loanId || generateLoanId(),
    customerDetails,
    businessDetails,
    bankDetails,
    loanDetails: derivedLoanDetails,
    guarantorDetails,
    groupDetails: groupDetails || {},
    guarantorFormPic: guarantorFormPic || null,
    pictures: pictures || {},
    status: "waiting for approval",
  };
}

// Submit a new loan by an authenticated CSO
router.post("/api/loans", authenticateCso, async (req, res) => {
  try {
    const payload = buildLoanPayload(req.body, req.cso);

    const created = await Loan.create(payload);
    return res.status(201).json(created);
  } catch (error) {
    if (error.code === 11000) {
      return res.status(409).json({ message: "Loan ID already exists" });
    }

    return res
      .status(400)
      .json({ message: error.message || "Unable to submit loan" });
  }
});

router.get("/api/loans/customer/:bvn", authenticateCso, async (req, res) => {
  try {
    const bvnParam = req.params.bvn ? String(req.params.bvn).trim() : "";

    if (!bvnParam) {
      return res.status(400).json({ message: "Customer BVN is required" });
    }

    const loans = await Loan.find({ "customerDetails.bvn": bvnParam }).sort({
      createdAt: -1,
    });
    return res.json(loans);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to fetch customer loans" });
  }
});

router.patch("/api/loans/:id/approve", async (req, res) => {
  try {
    const { amountApproved } = req.body || {};
    const parsedAmount = Number(amountApproved);

    if (!Number.isFinite(parsedAmount) || parsedAmount <= 0) {
      return res.status(400).json({
        message: "A valid amountApproved greater than zero is required",
      });
    }

    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    if (loan.status === "approved") {
      return res.status(400).json({ message: "Loan is already approved" });
    }

    const normalizedAmount = Number(parsedAmount.toFixed(2));
    const interest = Number((normalizedAmount * 0.1).toFixed(2));
    const amountToBePaid = Number((normalizedAmount + interest).toFixed(2));
    const repaymentCount =
      loan.loanDetails?.loanType === "weekly"
        ? WEEKLY_INSTALLMENT_COUNT
        : INSTALLMENT_DAYS_COUNT;
    const dailyAmount = Number((amountToBePaid / repaymentCount).toFixed(2));

    loan.status = "approved";
    loan.loanDetails = loan.loanDetails || {};
    loan.loanDetails.amountApproved = normalizedAmount;
    loan.loanDetails.interest = interest;
    loan.loanDetails.amountToBePaid = amountToBePaid;
    loan.loanDetails.dailyAmount = dailyAmount;
    loan.repaymentSchedule = generateRepaymentSchedule(
      new Date(),
      loan.loanDetails?.loanType
    );

    await loan.save();

    return res.json(loan);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to approve loan" });
  }
});

router.get("/api/loans/waiting", async (_req, res) => {
  try {
    const loans = await Loan.find({ status: "waiting for approval" }).sort({
      createdAt: -1,
    });
    return res.json(loans);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to fetch waiting loans" });
  }
});

router.get("/api/loans/approved", async (_req, res) => {
  try {
    const loans = await Loan.find({ status: "approved" }).sort({
      createdAt: -1,
    });
    return res.json(loans);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to fetch approved loans" });
  }
});

router.post("/api/loans/:id/payments", async (req, res) => {
  try {
    const { amount, date } = req.body || {};
    const parsedAmount = Number(amount);

    if (!Number.isFinite(parsedAmount) || parsedAmount <= 0) {
      return res.status(400).json({
        message: "A valid payment amount greater than zero is required",
      });
    }

    const paymentDate = date ? new Date(date) : new Date();

    if (Number.isNaN(paymentDate.getTime())) {
      return res
        .status(400)
        .json({ message: "A valid payment date is required" });
    }

    if (isWeekend(paymentDate)) {
      return res
        .status(400)
        .json({ message: "Payments cannot be recorded on weekends" });
    }

    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    loan.loanDetails = loan.loanDetails || {};
    loan.loanDetails.dailyPayment = Array.isArray(loan.loanDetails.dailyPayment)
      ? loan.loanDetails.dailyPayment
      : [];

    const normalizedAmount = Number(parsedAmount.toFixed(2));

    const amountToBePaid = Number(loan.loanDetails.amountToBePaid || 0);
    const currentPaid = Number(loan.loanDetails.amountPaidSoFar || 0);
    const outstandingBalance =
      amountToBePaid > 0
        ? Number((amountToBePaid - currentPaid).toFixed(2))
        : Infinity;

    if (
      amountToBePaid > 0 &&
      normalizedAmount > Math.max(outstandingBalance, 0)
    ) {
      return res.status(400).json({
        message: "Payment exceeds outstanding balance",
        outstandingBalance: Math.max(outstandingBalance, 0),
      });
    }

    loan.loanDetails.dailyPayment.push({
      amount: normalizedAmount,
      date: paymentDate,
    });

    const updatedPaid = Number((currentPaid + normalizedAmount).toFixed(2));
    loan.loanDetails.amountPaidSoFar = updatedPaid;

    if (amountToBePaid > 0 && Math.abs(updatedPaid - amountToBePaid) < 0.01) {
      loan.status = "fully paid";
    }

    await loan.save();

    return res.json({
      message: "Payment recorded successfully",
      dailyPayment: loan.loanDetails.dailyPayment,
      amountPaidSoFar: loan.loanDetails.amountPaidSoFar,
      status: loan.status,
    });
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to record payment" });
  }
});

router.get("/api/loans/:id/repayment/sync", async (req, res) => {
  try {
    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    const { schedule, amountPaidSoFar } = syncLoanRepaymentSchedule(loan);

    await loan.save();

    return res.json({
      message: "Repayment schedule synchronized successfully",
      repaymentSchedule: schedule,
      amountPaidSoFar,
    });
  } catch (error) {
    return res.status(400).json({
      message: error.message || "Unable to synchronize repayment schedule",
    });
  }
});

// Fetch loans submitted by authenticated CSO
router.get("/api/loans/me", authenticateCso, async (req, res) => {
  try {
    const loans = await Loan.find({ csoId: req.cso._id }).sort({
      createdAt: -1,
    });

    const uniqueByBvn = [];
    const seenBvns = new Set();

    for (const loan of loans) {
      const bvn = loan?.customerDetails?.bvn?.trim();

      if (!bvn) {
        uniqueByBvn.push(loan);
        continue;
      }

      if (!seenBvns.has(bvn)) {
        uniqueByBvn.push(loan);
        seenBvns.add(bvn);
      }
    }

    return res.json(uniqueByBvn);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to fetch loans" });
  }
});

// Fetch loans for a specific CSO (Admin only or authorized)
router.get("/api/loans/cso/:csoId", async (req, res) => {
  try {
    const { csoId } = req.params;

    // Ideally add admin authentication middleware here if available, e.g., authenticateAdmin
    // For now assuming it's protected or open as per existing pattern for some admin routes if any

    const loans = await Loan.find({ csoId }).sort({ createdAt: -1 });

    // Deduplicate by NIN if needed, similar to /me endpoint, or return all.
    // The /me endpoint deduplicates, so let's do the same here for consistency if that's the desired view.
    // However, for an admin view "All Loans", seeing every loan might be better.
    // But the user asked for "all loans submitted", and the /me endpoint implies "submitted" loans are filtered.
    // Let's stick to returning ALL loans for the admin view so they can see everything.
    // If deduplication is strictly required for "submitted" view, we can add it later.
    // Actually, looking at the request "all loans submitted, active loans...", it seems they want to see the actual loan records.
    // The /me endpoint deduplication seems specific to that view. Let's return all loans for now.

    return res.json(loans);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to fetch loans for CSO" });
  }
});

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

router.get("/api/loans/:id", async (req, res) => {
  try {
    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    return res.json(loan);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to fetch loan details" });
  }
});

router.patch("/api/loans/:id/reject", async (req, res) => {
  try {
    const { reason } = req.body || {};

    if (typeof reason !== "string" || reason.trim().length === 0) {
      return res
        .status(400)
        .json({ message: "A rejection reason is required" });
    }

    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    if (loan.status === "approved") {
      return res
        .status(400)
        .json({ message: "Approved loans cannot be rejected" });
    }

    if (loan.status === "rejected") {
      return res.status(400).json({ message: "Loan is already rejected" });
    }

    loan.status = "rejected";
    loan.rejectionReason = reason.trim();
    loan.repaymentSchedule = [];

    await loan.save();

    return res.json(loan);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to reject loan" });
  }
});

router.patch("/api/loans/:id/disburse", async (req, res) => {
  try {
    const { disbursementPicture } = req.body || {};

    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    if (loan.status !== "approved") {
      return res
        .status(400)
        .json({ message: "Only approved loans can be disbursed" });
    }

    if (disbursementPicture) {
      loan.loanDetails = loan.loanDetails || {};
      loan.loanDetails.disbursementPicture = disbursementPicture;
    }

    const approvedAmount = loan?.loanDetails?.amountApproved;

    if (!Number.isFinite(approvedAmount) || approvedAmount <= 0) {
      return res.status(400).json({
        message: "Loan must have an approved amount before disbursement",
      });
    }

    loan.loanDetails.amountDisbursed = approvedAmount;

    loan.status = "active loan";
    loan.disbursedAt = new Date();

    await loan.save();

    return res.json(loan);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to disburse loan" });
  }
});

// Fetch loans submitted by authenticated CSO
// router.get("/api/loans/me", authenticateCso, async (req, res) => {
//   try {
//     const loans = await Loan.find({ csoId: req.cso._id }).sort({ createdAt: -1 });
//     return res.json(loans);
//   } catch (error) {
//     return res.status(400).json({ message: error.message || "Unable to fetch loans" });
//   }
// });

// CSO Loan Metrics - Aggregated monthly view with pagination
router.get("/api/cso-loan-metrics", async (req, res) => {
  try {
    const now = new Date();
    const month = parseInt(req.query.month, 10) || now.getMonth() + 1;
    const year = parseInt(req.query.year, 10) || now.getFullYear();
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const limit = Math.min(
      100,
      Math.max(1, parseInt(req.query.limit, 10) || 20)
    );
    const skip = (page - 1) * limit;

    // Calculate date ranges
    const monthStart = new Date(year, month - 1, 1);
    const monthEnd = new Date(year, month, 0, 23, 59, 59, 999);
    const cumulativeEnd = monthEnd; // For cumulative balance calculation

    // Fetch all CSOs and relevant loans in parallel
    const [allCsos, monthLoans, cumulativeLoans] = await Promise.all([
      CSO.find({ isActive: true })
        .select("firstName lastName loanTarget disbursementTarget")
        .lean(),
      Loan.find({
        disbursedAt: { $gte: monthStart, $lte: monthEnd },
        status: { $in: ["active loan", "fully paid"] },
      })
        .select(
          "csoId loanDetails.amountDisbursed loanDetails.amountToBePaid loanDetails.formAmount loanDetails.dailyPayment"
        )
        .lean(),
      Loan.find({
        disbursedAt: { $lte: cumulativeEnd },
        status: { $in: ["active loan", "fully paid"] },
      })
        .select("csoId loanDetails.amountToBePaid loanDetails.dailyPayment")
        .lean(),
    ]);

    // Build CSO metrics map
    const csoMetrics = new Map();

    // Initialize all CSOs
    for (const cso of allCsos) {
      csoMetrics.set(cso._id.toString(), {
        csoId: cso._id,
        csoName: `${cso.firstName || ""} ${cso.lastName || ""}`.trim(),
        loanTarget: cso.loanTarget || 0,
        disbursementTarget: cso.disbursementTarget || 0,
        noOfLoans: 0,
        totalDisbursed: 0,
        amountToBePaid: 0,
        paymentsThisMonth: 0,
        formAmount: 0,
        cumulativeAmountToBePaid: 0,
        cumulativePayments: 0,
      });
    }

    // Process monthly loans
    for (const loan of monthLoans) {
      const csoIdStr = loan.csoId?.toString();
      if (!csoIdStr || !csoMetrics.has(csoIdStr)) continue;

      const metrics = csoMetrics.get(csoIdStr);
      metrics.noOfLoans += 1;
      metrics.totalDisbursed += Number(loan.loanDetails?.amountDisbursed) || 0;
      metrics.amountToBePaid += Number(loan.loanDetails?.amountToBePaid) || 0;
      metrics.formAmount +=
        Number(loan.loanDetails?.formAmount) || FORM_AMOUNT_DEFAULT;

      // Sum payments made in this month
      const payments = Array.isArray(loan.loanDetails?.dailyPayment)
        ? loan.loanDetails.dailyPayment
        : [];
      for (const payment of payments) {
        const paymentDate = new Date(payment.date);
        if (paymentDate >= monthStart && paymentDate <= monthEnd) {
          metrics.paymentsThisMonth += Number(payment.amount) || 0;
        }
      }
    }

    // Process cumulative data for loan balance
    for (const loan of cumulativeLoans) {
      const csoIdStr = loan.csoId?.toString();
      if (!csoIdStr || !csoMetrics.has(csoIdStr)) continue;

      const metrics = csoMetrics.get(csoIdStr);
      metrics.cumulativeAmountToBePaid +=
        Number(loan.loanDetails?.amountToBePaid) || 0;

      // Sum all payments up to end of selected month
      const payments = Array.isArray(loan.loanDetails?.dailyPayment)
        ? loan.loanDetails.dailyPayment
        : [];
      for (const payment of payments) {
        const paymentDate = new Date(payment.date);
        if (paymentDate <= cumulativeEnd) {
          metrics.cumulativePayments += Number(payment.amount) || 0;
        }
      }
    }

    // Convert to array and calculate final values
    const metricsArray = Array.from(csoMetrics.values()).map((m) => ({
      csoId: m.csoId,
      csoName: m.csoName,
      noOfLoans: m.noOfLoans,
      totalDisbursed: Number(m.totalDisbursed.toFixed(2)),
      amountToBePaid: Number(m.amountToBePaid.toFixed(2)),
      paymentsThisMonth: Number(m.paymentsThisMonth.toFixed(2)),
      loanBalance: Number(
        (m.cumulativeAmountToBePaid - m.cumulativePayments).toFixed(2)
      ),
      formAmount: Number(m.formAmount.toFixed(2)),
      loanTarget: m.loanTarget,
      disbursementTarget: m.disbursementTarget,
      targetMet: m.noOfLoans >= m.loanTarget && m.loanTarget > 0,
    }));

    // Sort by CSO name
    metricsArray.sort((a, b) => a.csoName.localeCompare(b.csoName));

    // Pagination
    const total = metricsArray.length;
    const totalPages = Math.ceil(total / limit);
    const paginatedData = metricsArray.slice(skip, skip + limit);

    return res.json({
      data: paginatedData,
      pagination: {
        page,
        limit,
        total,
        totalPages,
      },
      month,
      year,
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch CSO loan metrics" });
  }
});

// Customer Loans - Weekly payment breakdown view with pagination
router.get("/api/customer-loan-weekly", async (req, res) => {
  try {
    // Parse week start date (Monday) - defaults to current week
    let weekStart;
    if (req.query.weekStart) {
      weekStart = new Date(req.query.weekStart);
      if (isNaN(weekStart.getTime())) {
        return res.status(400).json({ message: "Invalid weekStart date" });
      }
    } else {
      // Get current week's Monday
      const today = new Date();
      const dayOfWeek = today.getDay();
      const diff = dayOfWeek === 0 ? -6 : 1 - dayOfWeek; // If Sunday, go back 6 days, else go to Monday
      weekStart = new Date(today);
      weekStart.setDate(today.getDate() + diff);
    }
    weekStart.setHours(0, 0, 0, 0);

    // Calculate end of week (Friday end of day)
    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + 4); // Friday
    weekEnd.setHours(23, 59, 59, 999);

    // Pagination
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const limit = Math.min(
      100,
      Math.max(1, parseInt(req.query.limit, 10) || 20)
    );
    const skip = (page - 1) * limit;

    // Build query filter
    const queryFilter = {
      status: { $in: ["active loan", "fully paid"] },
    };

    // Filter by CSO if provided
    if (req.query.csoId) {
      queryFilter.csoId = req.query.csoId;
    }

    // Search by customer name if provided
    if (req.query.search) {
      const searchRegex = new RegExp(req.query.search.trim(), "i");
      queryFilter.$or = [
        { "customerDetails.firstName": searchRegex },
        { "customerDetails.lastName": searchRegex },
      ];
    }

    // Fetch active loans with only necessary fields for speed
    const [loans, totalCount] = await Promise.all([
      Loan.find(queryFilter)
        .select(
          "customerDetails.firstName customerDetails.lastName loanDetails.amountDisbursed loanDetails.amountToBePaid loanDetails.amountPaidSoFar loanDetails.dailyPayment disbursedAt loanId csoId csoName"
        )
        .sort({ disbursedAt: -1 })
        .skip(skip)
        .limit(limit)
        .lean(),
      Loan.countDocuments(queryFilter),
    ]);

    // Generate weekday dates
    const weekDays = [];
    for (let i = 0; i < 5; i++) {
      const day = new Date(weekStart);
      day.setDate(weekStart.getDate() + i);
      weekDays.push({
        date: day,
        key: day.toISOString().slice(0, 10),
        label: ["Mon", "Tue", "Wed", "Thu", "Fri"][i],
      });
    }

    // Process loans and map payments to weekdays
    const data = loans.map((loan) => {
      const payments = Array.isArray(loan.loanDetails?.dailyPayment)
        ? loan.loanDetails.dailyPayment
        : [];

      // Map payments to weekday keys
      const paymentsByDay = {};
      for (const day of weekDays) {
        paymentsByDay[day.label] = 0;
      }

      for (const payment of payments) {
        const paymentDate = new Date(payment.date);
        const paymentKey = paymentDate.toISOString().slice(0, 10);

        for (const day of weekDays) {
          if (day.key === paymentKey) {
            paymentsByDay[day.label] += Number(payment.amount) || 0;
            break;
          }
        }
      }

      // Calculate expected end date (30 days after disbursement)
      const disbursedAt = loan.disbursedAt ? new Date(loan.disbursedAt) : null;
      let expectedEndDate = null;
      if (disbursedAt) {
        expectedEndDate = new Date(disbursedAt);
        expectedEndDate.setDate(disbursedAt.getDate() + 30);
      }

      return {
        loanId: loan.loanId,
        customerName: `${loan.customerDetails?.firstName || ""} ${
          loan.customerDetails?.lastName || ""
        }`.trim(),
        amountDisbursed: Number(loan.loanDetails?.amountDisbursed || 0),
        amountToBePaid: Number(loan.loanDetails?.amountToBePaid || 0),
        amountPaid: Number(loan.loanDetails?.amountPaidSoFar || 0),
        startDate: disbursedAt ? disbursedAt.toISOString().slice(0, 10) : null,
        expectedEndDate: expectedEndDate
          ? expectedEndDate.toISOString().slice(0, 10)
          : null,
        payments: paymentsByDay,
      };
    });

    const totalPages = Math.ceil(totalCount / limit);

    return res.json({
      data,
      pagination: {
        page,
        limit,
        total: totalCount,
        totalPages,
      },
      week: {
        start: weekStart.toISOString().slice(0, 10),
        end: weekEnd.toISOString().slice(0, 10),
        days: weekDays.map((d) => ({ label: d.label, date: d.key })),
      },
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch customer loans" });
  }
});

// Overdue Loans - Loans past 30 days
router.get("/api/overdue-loans", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const limit = Math.min(
      100,
      Math.max(1, parseInt(req.query.limit, 10) || 20)
    );
    const skip = (page - 1) * limit;

    // Search query
    const search = req.query.search || "";
    const csoId = req.query.csoId || "";

    // Calculate date 30 days ago
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

    const query = {
      status: "active loan",
      disbursedAt: { $lt: thirtyDaysAgo },
    };

    if (csoId) {
      query.csoId = csoId;
    }

    if (search) {
      const searchRegex = new RegExp(search.trim(), "i");
      query.$or = [
        { "customerDetails.firstName": searchRegex },
        { "customerDetails.lastName": searchRegex },
      ];
    }

    const [loans, total] = await Promise.all([
      Loan.find(query)
        .select(
          "customerDetails.firstName customerDetails.lastName loanDetails.amountToBePaid loanDetails.amountPaidSoFar loanDetails.amountDisbursed disbursedAt loanId"
        )
        .sort({ disbursedAt: 1 }) // Show oldest loans first
        .skip(skip)
        .limit(limit)
        .lean(),
      Loan.countDocuments(query),
    ]);

    const now = new Date();
    const data = loans.map((loan) => {
      const disbursedAt = new Date(loan.disbursedAt);
      const expectedEndDate = new Date(disbursedAt);
      expectedEndDate.setDate(disbursedAt.getDate() + 30);

      const diffTime = Math.abs(now - expectedEndDate);
      const overDueCount = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

      return {
        loanId: loan.loanId,
        customerName: `${loan.customerDetails?.firstName || ""} ${
          loan.customerDetails?.lastName || ""
        }`.trim(),
        amountToBePaid: loan.loanDetails?.amountToBePaid || 0,
        amountPaid: loan.loanDetails?.amountPaidSoFar || 0,
        amountDisbursed: loan.loanDetails?.amountDisbursed || 0,
        // Calculate balance: ToBePaid - Paid
        loanBalance:
          (loan.loanDetails?.amountToBePaid || 0) -
          (loan.loanDetails?.amountPaidSoFar || 0),
        overDueCount: overDueCount,
        disbursedAt: disbursedAt.toISOString(),
      };
    });

    const totalPages = Math.ceil(total / limit);

    return res.json({
      data,
      pagination: {
        page,
        limit,
        total,
        totalPages,
      },
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch overdue loans" });
  }
});

// Fetch aggregated customer summary (Admin)
router.get("/api/admin/customers", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const search = req.query.search ? req.query.search.trim() : "";
    const skip = (page - 1) * limit;

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const matchStage = {};
    if (search) {
      matchStage.$or = [
        { "customerDetails.firstName": { $regex: search, $options: "i" } },
        { "customerDetails.lastName": { $regex: search, $options: "i" } },
        { "customerDetails.bvn": { $regex: search, $options: "i" } },
      ];
    }

    const aggregation = await Loan.aggregate([
      { $match: matchStage },
      { $sort: { createdAt: -1 } },
      {
        $group: {
          _id: "$customerDetails.bvn",
          details: { $first: "$customerDetails" },
          loans: { $push: "$$ROOT" },
          loanCount: { $sum: 1 },
        },
      },
      {
        $facet: {
          metadata: [{ $count: "total" }],
          data: [
            { $sort: { "details.firstName": 1 } },
            { $skip: skip },
            { $limit: limit },
            {
              $addFields: {
                activeLoan: {
                  $arrayElemAt: [
                    {
                      $filter: {
                        input: "$loans",
                        as: "loan",
                        cond: { $eq: ["$$loan.status", "active loan"] },
                      },
                    },
                    0,
                  ],
                },
                allDefaultsCount: {
                  $reduce: {
                    input: "$loans",
                    initialValue: 0,
                    in: {
                      $add: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.repaymentSchedule",
                              as: "item",
                              cond: {
                                $and: [
                                  { $eq: ["$$item.status", "pending"] },
                                  { $lte: ["$$item.date", today] },
                                ],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
                totalScheduleItems: {
                  $reduce: {
                    input: "$loans",
                    initialValue: 0,
                    in: {
                      $add: ["$$value", { $size: "$$this.repaymentSchedule" }],
                    },
                  },
                },
              },
            },
            {
              $project: {
                _id: 0,
                bvn: "$_id",
                customerName: {
                  $concat: ["$details.firstName", " ", "$details.lastName"],
                },
                loansCollected: "$loanCount",
                defaultsCount: "$allDefaultsCount",
                performance: {
                  $cond: [
                    { $eq: ["$totalScheduleItems", 0] },
                    100,
                    {
                      $multiply: [
                        {
                          $subtract: [
                            1,
                            {
                              $divide: [
                                "$allDefaultsCount",
                                "$totalScheduleItems",
                              ],
                            },
                          ],
                        },
                        100,
                      ],
                    },
                  ],
                },
                activeLoanDetails: {
                  $cond: [
                    { $ifNull: ["$activeLoan", false] },
                    {
                      amountToBePaid: "$activeLoan.loanDetails.amountToBePaid",
                      amountPaidSoFar:
                        "$activeLoan.loanDetails.amountPaidSoFar",
                      balance: {
                        $subtract: [
                          "$activeLoan.loanDetails.amountToBePaid",
                          "$activeLoan.loanDetails.amountPaidSoFar",
                        ],
                      },
                      startDate: "$activeLoan.disbursedAt",
                    },
                    null,
                  ],
                },
                hasPaymentDueToday: {
                  $cond: [
                    { $ifNull: ["$activeLoan", false] },
                    {
                      $gt: [
                        {
                          $size: {
                            $filter: {
                              input: "$activeLoan.repaymentSchedule",
                              as: "sched",
                              cond: {
                                $and: [
                                  { $eq: ["$$sched.status", "pending"] },
                                  { $lte: ["$$sched.date", today] },
                                ],
                              },
                            },
                          },
                        },
                        0,
                      ],
                    },
                    false,
                  ],
                },
              },
            },
            {
              $addFields: {
                status: {
                  $cond: [
                    { $not: ["$activeLoanDetails"] },
                    "No open loan",
                    {
                      $cond: [
                        { $eq: ["$hasPaymentDueToday", true] },
                        "Defaulting",
                        "Not defaulting",
                      ],
                    },
                  ],
                },
                "activeLoanDetails.endDate": {
                  $add: ["$activeLoanDetails.startDate", 2592000000],
                },
              },
            },
          ],
        },
      },
    ]);

    const metadata = aggregation[0].metadata[0] || { total: 0 };
    const data = aggregation[0].data;

    return res.json({
      customers: data,
      pagination: {
        total: metadata.total,
        page,
        limit,
        pages: Math.ceil(metadata.total / limit),
      },
    });
  } catch (error) {
    console.error("Aggregation error:", error);
    return res
      .status(500)
      .json({ message: "Unable to fetch customer summary" });
  }
});

// Fetch all loans for a specific customer (Admin)
router.get("/api/admin/customers/:bvn/loans", async (req, res) => {
  try {
    const bvn = req.params.bvn ? String(req.params.bvn).trim() : "";

    if (!bvn) {
      return res.status(400).json({ message: "Customer BVN is required" });
    }

    const loans = await Loan.find({ "customerDetails.bvn": bvn })
      .sort({ createdAt: -1 })
      .lean();

    const formattedLoans = loans.map((loan) => {
      const customer = loan.customerDetails || {};
      const loanDetails = loan.loanDetails || {};
      const paymentsArray = Array.isArray(loanDetails.dailyPayment)
        ? loanDetails.dailyPayment
        : [];

      const startDate = loan.disbursedAt || loan.createdAt || null;

      let endDate = null;
      if (loan.status === "fully paid" && paymentsArray.length > 0) {
        const lastPayment = paymentsArray.reduce((latest, payment) => {
          if (!payment?.date) {
            return latest;
          }
          const paymentDate = new Date(payment.date);
          if (!latest) {
            return paymentDate;
          }
          return paymentDate > latest ? paymentDate : latest;
        }, null);
        endDate = lastPayment || startDate;
      } else if (startDate) {
        const computed = new Date(startDate);
        computed.setDate(computed.getDate() + 30);
        endDate = computed;
      }

      const amountToBePaid = Number(loanDetails.amountToBePaid) || 0;
      const amountPaidSoFar = Number(loanDetails.amountPaidSoFar) || 0;
      const balance = Math.max(amountToBePaid - amountPaidSoFar, 0);

      return {
        id: loan._id,
        loanId: loan.loanId,
        customerName: [customer.firstName, customer.lastName]
          .filter(Boolean)
          .join(" ")
          .trim(),
        amountToBePaid,
        amountPaidSoFar,
        balance,
        startDate: startDate ? new Date(startDate).toISOString() : null,
        endDate: endDate ? new Date(endDate).toISOString() : null,
        status: loan.status,
        loanType: loanDetails.loanType || "daily",
        dailyPayment: paymentsArray
          .filter((payment) => payment?.date)
          .map((payment) => ({
            date: new Date(payment.date).toISOString(),
            amount: Number(payment.amount) || 0,
          })),
      };
    });

    return res.json({
      bvn,
      customerName:
        formattedLoans[0]?.customerName || loans[0]?.customerDetails?.firstName
          ? [
              loans[0]?.customerDetails?.firstName,
              loans[0]?.customerDetails?.lastName,
            ]
              .filter(Boolean)
              .join(" ")
              .trim()
          : "",
      loans: formattedLoans,
    });
  } catch (error) {
    console.error("Failed to fetch customer loans:", error);
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch customer loans" });
  }
});

// Fetch the most recent customer submission details (Admin)
router.get("/api/admin/customers/:bvn/details", async (req, res) => {
  try {
    const bvn = req.params.bvn ? String(req.params.bvn).trim() : "";

    if (!bvn) {
      return res.status(400).json({ message: "Customer BVN is required" });
    }

    const latestLoan = await Loan.findOne({ "customerDetails.bvn": bvn })
      .sort({ createdAt: -1 })
      .lean();

    if (!latestLoan) {
      return res.status(404).json({ message: "No customer records found" });
    }

    return res.json({
      bvn,
      loanId: latestLoan.loanId,
      createdAt: latestLoan.createdAt,
      updatedAt: latestLoan.updatedAt,
      customerDetails: latestLoan.customerDetails || {},
      businessDetails: latestLoan.businessDetails || {},
      bankDetails: latestLoan.bankDetails || {},
      guarantorDetails: latestLoan.guarantorDetails || {},
      loanDetails: latestLoan.loanDetails || {},
      groupDetails: latestLoan.groupDetails || {},
      pictures: latestLoan.pictures || {},
      csoDetails: {
        id: latestLoan.csoId || "",
        name: latestLoan.csoName || "",
        branch: latestLoan.branch || "",
        signature: latestLoan.csoSignature || "",
      },
    });
  } catch (error) {
    console.error("Failed to fetch customer details:", error);
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch customer details" });
  }
});

// Get customers (loans) for a specific CSO
router.get("/api/loans/cso/:csoId/customers", async (req, res) => {
  try {
    const { csoId } = req.params;
    const { search, groupId } = req.query;

    const query = {
      csoId,
      status: { $in: ["active loan", "fully paid", "approved"] },
    };

    if (search) {
      const searchRegex = new RegExp(search, "i");
      query.$or = [
        { "customerDetails.firstName": searchRegex },
        { "customerDetails.lastName": searchRegex },
        { "customerDetails.bvn": searchRegex },
      ];
    }

    if (groupId) {
      if (groupId === "ungrouped") {
        query["groupDetails.groupId"] = { $exists: false };
      } else {
        query["groupDetails.groupId"] = groupId;
      }
    }

    const loans = await Loan.find(query)
      .select("loanId customerDetails loanDetails groupDetails status")
      .sort({ "customerDetails.firstName": 1 });

    res.json(loans);
  } catch (error) {
    res
      .status(500)
      .json({ message: error.message || "Unable to fetch customers" });
  }
});

// Bulk assign customers to a group
router.post("/api/loans/assign-group", async (req, res) => {
  try {
    const { loanIds, groupLeaderId } = req.body;

    if (!Array.isArray(loanIds) || loanIds.length === 0) {
      return res.status(400).json({ message: "No customers selected" });
    }

    if (!groupLeaderId) {
      return res.status(400).json({ message: "Group leader is required" });
    }

    const GroupLeader = require("../models/groupLeader"); // Dynamic import to avoid circular dependency issues if any
    const groupLeader = await GroupLeader.findById(groupLeaderId);

    if (!groupLeader) {
      return res.status(404).json({ message: "Group leader not found" });
    }

    const groupDetails = {
      groupName: groupLeader.groupName,
      leaderName: `${groupLeader.firstName} ${groupLeader.lastName}`,
      address: groupLeader.address,
      groupId: groupLeader._id,
      mobileNo: groupLeader.phone,
    };

    await Loan.updateMany(
      { _id: { $in: loanIds } },
      { $set: { groupDetails } }
    );

    res.json({ message: "Customers transferred successfully" });
  } catch (error) {
    res
      .status(500)
      .json({ message: error.message || "Unable to transfer customers" });
  }
});

module.exports = router;
