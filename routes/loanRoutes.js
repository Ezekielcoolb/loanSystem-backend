async function resolveInterestRate() {
  try {
    const latest = await Interest.findOne({}, null, {
      sort: { createdAt: -1 },
    }).lean();
    const rate = Number(latest?.amount);

    if (Number.isFinite(rate) && rate >= 0) {
      return rate;
    }
  } catch (error) {
    console.error("Failed to resolve interest rate", error);
  }

  return DEFAULT_INTEREST_RATE;
}

const express = require("express");
const mongoose = require("mongoose");
const Loan = require("../models/loan");
const CSO = require("../models/cso");
const Branch = require("../models/branch");
const Report = require("../models/Report");
const Interest = require("../models/NewInterest");
const Holiday = require("../models/Holiday");
const authenticateCso = require("../middleware/authenticateCso");

const router = express.Router();

const WORKING_DAYS_COUNT = 23;
const INSTALLMENT_DAYS_COUNT = 22;
const WEEKLY_INSTALLMENT_COUNT = 5;
const FORM_AMOUNT_DEFAULT = 2000;
const DEFAULT_INTEREST_RATE = 0.1;
const ACTIVE_LOAN_STATUSES = ["approved", "active loan", "fully paid"];
const MS_PER_DAY = 1000 * 60 * 60 * 24;
const MS_PER_WEEK = MS_PER_DAY * 7;

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

function getWeekdaysBetweenSync(startDate, endDate, holidaySet) {
  let count = 0;
  const current = new Date(startDate);
  const normalizedEnd = normalizeDate(endDate);
  while (current <= normalizedEnd) {
    const day = current.getDay();
    const currentTime = normalizeDate(current).getTime();
    if (day !== 0 && day !== 6 && !holidaySet.has(currentTime)) {
      count++;
    }
    current.setDate(current.getDate() + 1);
  }
  return count;
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

function getLocalToday() {
  const date = new Date();
  date.setHours(0, 0, 0, 0);
  return date;
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

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function sanitizeSearchTerm(value) {
  if (typeof value !== "string") {
    return "";
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : "";
}

function getDaysInMonth(year, month) {
  return new Date(year, month, 0).getDate();
}

function buildWeekDefinitions(year, month) {
  const daysInMonth = getDaysInMonth(year, month);
  const weeksCount = Math.ceil(daysInMonth / 7);
  const definitions = [];

  for (let index = 0; index < weeksCount; index += 1) {
    const startDay = index * 7 + 1;
    const endDay = Math.min((index + 1) * 7, daysInMonth);

    const startDate = new Date(year, month - 1, startDay);
    startDate.setHours(0, 0, 0, 0);
    const endDate = new Date(year, month - 1, endDay);
    endDate.setHours(23, 59, 59, 999);

    definitions.push({
      index,
      label: `Week ${index + 1}`,
      startDay,
      endDay,
      startIso: startDate.toISOString().slice(0, 10),
      endIso: endDate.toISOString().slice(0, 10),
    });
  }

  return definitions;
}

function createWeekTemplate(definitions) {
  return definitions.map((definition) => ({
    index: definition.index,
    label: definition.label,
    startDay: definition.startDay,
    endDay: definition.endDay,
    startDate: definition.startIso,
    endDate: definition.endIso,
    count: 0,
  }));
}

function computeAdminDateRange({
  rangeParam,
  dateParam,
  fromParam,
  toParam,
  fallbackYear,
  fallbackMonthIndex,
}) {
  const today = normalizeDate(new Date());
  let startDate = null;
  let endDate = null;

  const parsedDateParam = normalizeDate(dateParam);
  const parsedFromParam = normalizeDate(fromParam);
  const parsedToParam = normalizeDate(toParam);

  if (rangeParam === "today") {
    startDate = today;
    endDate = addDays(today, 1);
  } else if (rangeParam === "yesterday") {
    endDate = today;
    startDate = addDays(endDate, -1);
  } else if (rangeParam === "week" || rangeParam === "thisweek") {
    endDate = addDays(today, 1);
    startDate = addDays(endDate, -7);
  } else if (parsedDateParam) {
    startDate = parsedDateParam;
    endDate = addDays(parsedDateParam, 1);
  }

  if (parsedFromParam) {
    startDate = parsedFromParam;
    if (!parsedToParam) {
      endDate = addDays(parsedFromParam, 1);
    }
  }

  if (parsedToParam) {
    endDate = addDays(parsedToParam, 1);
    if (!startDate) {
      startDate = addDays(endDate, -1);
    }
  }

  if (!startDate || !endDate) {
    startDate = normalizeDate(new Date(fallbackYear, fallbackMonthIndex, 1));
    endDate = normalizeDate(new Date(fallbackYear, fallbackMonthIndex + 1, 1));
  }

  if (startDate >= endDate) {
    endDate = addDays(startDate, 1);
  }

  const effectiveRange =
    rangeParam || parsedFromParam || parsedToParam || parsedDateParam
      ? rangeParam || "custom"
      : "month";

  return {
    startDate,
    endDate,
    effectiveRange,
  };
}

function getStartOfWeek(date) {
  const base = normalizeDate(date);

  if (!base) {
    return null;
  }

  const day = base.getDay();
  const diff = day === 0 ? -6 : 1 - day; // Monday as start of week
  const start = new Date(base);
  start.setDate(start.getDate() + diff);
  return start;
}

function deriveTimeframeRange(key, referenceDate) {
  const now = new Date(referenceDate);
  const startOfToday = normalizeDate(now);

  switch (key) {
    case "today":
      return { start: startOfToday, end: now };
    case "yesterday": {
      const start = addDays(startOfToday, -1);
      return { start, end: startOfToday };
    }
    case "thisWeek": {
      const start = getStartOfWeek(now) || startOfToday;
      return { start, end: now };
    }
    case "thisMonth": {
      const start = new Date(now.getFullYear(), now.getMonth(), 1);
      return { start, end: now };
    }
    case "thisYear": {
      const start = new Date(now.getFullYear(), 0, 1);
      return { start, end: now };
    }
    case "overall":
    default:
      return { start: null, end: null };
  }
}

function isWithinRange(date, range) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return false;
  }

  if (range?.start && date < range.start) {
    return false;
  }

  if (range?.end && date > range.end) {
    return false;
  }

  return true;
}

function toNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function formatDisbursementEntry(loan) {
  if (!loan) {
    return null;
  }

  const customerDetails = loan.customerDetails || {};
  const loanDetails = loan.loanDetails || {};
  const disbursedAt =
    loan.disbursedAt instanceof Date
      ? loan.disbursedAt.toISOString()
      : loan.disbursedAt || null;

  const customerName = [customerDetails.firstName, customerDetails.lastName]
    .filter(Boolean)
    .join(" ")
    .trim();

  return {
    id: loan._id?.toString?.() || String(loan._id || ""),
    loanId: loan.loanId,
    customerName,
    csoName: loan.csoName || "",
    csoId: loan.csoId,
    amountDisbursed: Number(loanDetails.amountDisbursed || 0),
    amountToBePaid: Number(loanDetails.amountToBePaid || 0),
    loanType: loanDetails.loanType || "",
    adminFee: Number(loanDetails.loanAppForm || 0),
    disbursedAt,
    status: loan.status || "",
  };
}

function formatCollectionEntry(document) {
  if (!document) {
    return null;
  }

  const customerDetails = document.customerDetails || {};
  const payment = document.payment || {};

  const customerName = [customerDetails.firstName, customerDetails.lastName]
    .filter(Boolean)
    .join(" ")
    .trim();

  const normalizedPaymentDate = normalizeDate(payment.date);
  const paymentIso = normalizedPaymentDate
    ? new Date(normalizedPaymentDate).toISOString()
    : null;

  return {
    id:
      document._id?.toString?.() || (document._id ? String(document._id) : ""),
    loanId: document.loanId,
    customerName,
    csoName: document.csoName || "",
    csoId: document.csoId,
    amountPaid: Number(payment.amount || 0),
    paymentDate: paymentIso,
  };
}

function toObjectId(value) {
  if (!value) {
    return null;
  }

  if (value instanceof mongoose.Types.ObjectId) {
    return value;
  }

  if (mongoose.Types.ObjectId.isValid(value)) {
    return new mongoose.Types.ObjectId(value);
  }

  return null;
}

function formatDateFilterMeta(startDate, endDate) {
  const startKey = formatDateKey(startDate);
  const endKey = formatDateKey(addDays(endDate, -1));

  return {
    startDate: startKey,
    endDate: endKey,
  };
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

function sanitizeDailyPayments(loan) {
  if (!loan) {
    return { total: 0, payments: [], changed: false };
  }

  if (!loan.loanDetails || typeof loan.loanDetails !== "object") {
    loan.loanDetails = {};
  }

  const existingPayments = Array.isArray(loan.loanDetails.dailyPayment)
    ? loan.loanDetails.dailyPayment
    : [];

  const uniquePayments = [];
  const seenIds = new Set();
  const seenCompositeKeys = new Set();
  let changed = false;

  existingPayments.forEach((rawPayment) => {
    if (!rawPayment) {
      changed = true;
      return;
    }

    const amount = normalizeAmount(rawPayment.amount);

    if (!amount || amount <= 0) {
      changed = true;
      return;
    }

    const parsedDate = new Date(
      rawPayment.date instanceof Date
        ? rawPayment.date.getTime()
        : rawPayment.date
    );

    if (!parsedDate || Number.isNaN(parsedDate.getTime())) {
      changed = true;
      return;
    }

    let storedId = rawPayment._id;
    let identifier = null;

    if (storedId instanceof mongoose.Types.ObjectId) {
      identifier = storedId.toHexString();
    } else if (typeof storedId === "string" && storedId.trim().length > 0) {
      const trimmed = storedId.trim();
      identifier = trimmed;

      if (mongoose.Types.ObjectId.isValid(trimmed)) {
        storedId = new mongoose.Types.ObjectId(trimmed);
      } else {
        storedId = trimmed;
      }
    } else if (storedId && typeof storedId === "object" && storedId.toString) {
      identifier = storedId.toString();
    }

    const compositeKey = `${parsedDate.toISOString()}|${amount.toFixed(2)}`;

    if (identifier && seenIds.has(identifier)) {
      changed = true;
      return;
    }

    if (seenCompositeKeys.has(compositeKey) && !identifier) {
      changed = true;
      return;
    }

    if (!identifier) {
      const newObjectId = new mongoose.Types.ObjectId();
      storedId = newObjectId;
      identifier = newObjectId.toHexString();
      changed = true;
    }

    if (!(rawPayment.date instanceof Date)) {
      changed = true;
    }

    if (rawPayment.amount !== amount) {
      changed = true;
    }

    seenIds.add(identifier);
    seenCompositeKeys.add(compositeKey);

    uniquePayments.push({
      _id: storedId,
      amount,
      date: parsedDate,
    });
  });

  uniquePayments.sort((first, second) => {
    const dateDiff = first.date - second.date;

    if (dateDiff !== 0) {
      return dateDiff;
    }

    return (first._id || "").localeCompare(second._id || "");
  });

  const signatureOf = (payment) => {
    const iso =
      payment.date instanceof Date
        ? payment.date.toISOString()
        : new Date(payment.date).toISOString();
    const normalizedAmount = normalizeAmount(payment.amount) || 0;
    const id =
      payment._id instanceof mongoose.Types.ObjectId
        ? payment._id.toHexString()
        : typeof payment._id === "string"
        ? payment._id
        : payment._id && payment._id.toString
        ? payment._id.toString()
        : "";

    return `${iso}|${normalizedAmount.toFixed(2)}|${id}`;
  };

  const originalSignatures = existingPayments
    .reduce((accumulator, payment) => {
      if (!payment) {
        return accumulator;
      }

      const amt = normalizeAmount(payment.amount);

      if (!amt || amt <= 0) {
        return accumulator;
      }

      const parsed = new Date(
        payment.date instanceof Date ? payment.date.getTime() : payment.date
      );

      if (!parsed || Number.isNaN(parsed.getTime())) {
        return accumulator;
      }

      let id = "";

      if (payment._id instanceof mongoose.Types.ObjectId) {
        id = payment._id.toHexString();
      } else if (
        typeof payment._id === "string" &&
        payment._id.trim().length > 0
      ) {
        id = payment._id.trim();
      } else if (
        payment._id &&
        typeof payment._id === "object" &&
        payment._id.toString
      ) {
        id = payment._id.toString();
      }

      accumulator.push(`${parsed.toISOString()}|${amt.toFixed(2)}|${id}`);
      return accumulator;
    }, [])
    .sort();

  const sanitizedPayments = uniquePayments.map((payment) => ({
    _id: payment._id,
    amount: payment.amount,
    date: payment.date,
  }));

  const sanitizedSignatures = sanitizedPayments.map(signatureOf).sort();

  if (!changed) {
    if (originalSignatures.length !== sanitizedSignatures.length) {
      changed = true;
    } else {
      for (let index = 0; index < sanitizedSignatures.length; index += 1) {
        if (sanitizedSignatures[index] !== originalSignatures[index]) {
          changed = true;
          break;
        }
      }
    }
  }

  if (changed) {
    loan.loanDetails.dailyPayment = sanitizedPayments;
    loan.markModified("loanDetails.dailyPayment");
  }

  const normalizedTotal =
    normalizeAmount(
      sanitizedPayments.reduce(
        (sum, payment) => sum + (Number(payment.amount) || 0),
        0
      )
    ) || 0;

  if (loan.loanDetails.amountPaidSoFar !== normalizedTotal) {
    loan.loanDetails.amountPaidSoFar = normalizedTotal;
    loan.markModified("loanDetails.amountPaidSoFar");
  }

  return {
    total: normalizedTotal,
    payments: sanitizedPayments,
    changed,
  };
}

async function resolveHolidayMap(startDate, endDate) {
  const start = normalizeDate(startDate) || new Date();
  const end = normalizeDate(endDate) || start;

  const holidayQuery = {
    $or: [{ holiday: { $gte: start, $lte: end } }, { isRecurring: true }],
  };

  const holidays = await Holiday.find(holidayQuery).lean();

  const byDate = new Map();
  const recurring = new Map();

  holidays.forEach((holiday) => {
    if (!holiday) {
      return;
    }

    const iso = formatDateKey(holiday.holiday);

    if (iso) {
      byDate.set(iso, holiday.reason || "Holiday");
    }

    if (holiday.isRecurring && holiday.recurringKey) {
      recurring.set(holiday.recurringKey, holiday.reason || "Holiday");
    }
  });

  return { byDate, recurring };
}

function applyHolidayStatus(entry, holidayMaps) {
  if (!entry || !holidayMaps) {
    return entry;
  }

  const { byDate, recurring } = holidayMaps;
  const isoKey = formatDateKey(entry.date);

  let reason = null;

  if (isoKey && byDate?.has(isoKey)) {
    reason = byDate.get(isoKey);
  } else if (entry.date instanceof Date && recurring?.size) {
    const month = String(entry.date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(entry.date.getUTCDate()).padStart(2, "0");
    const recurringKey = `${month}-${day}`;

    if (recurring.has(recurringKey)) {
      reason = recurring.get(recurringKey);
    }
  }

  if (reason) {
    entry.status = "holiday";
    entry.amountPaid = 0;
    entry.holidayReason = reason;
  }

  return entry;
}

async function initializeRepaymentSchedule(loan, fallbackStartDate) {
  const hasExistingSchedule =
    Array.isArray(loan.repaymentSchedule) && loan.repaymentSchedule.length > 0;
  const startDate = normalizeDate(fallbackStartDate) || new Date();
  const baseEntries = hasExistingSchedule
    ? loan.repaymentSchedule
    : generateRepaymentSchedule(startDate, loan.loanDetails?.loanType);

  const lookupEndDate = addDays(startDate, WORKING_DAYS_COUNT * 2);
  const holidayMaps = await resolveHolidayMap(startDate, lookupEndDate);

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

      const nextEntry = {
        date: normalizedDate,
        status,
        amountPaid: 0,
        holidayReason: entry?.holidayReason,
      };

      applyHolidayStatus(nextEntry, holidayMaps);
      return nextEntry;
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

async function syncLoanRepaymentSchedule(loan) {
  const dailyAmount = normalizeAmount(loan?.loanDetails?.dailyAmount);

  if (!dailyAmount || dailyAmount <= 0) {
    throw new Error("Loan is missing a valid dailyAmount");
  }

  const { payments: sanitizedExistingPayments, changed: paymentsChanged } =
    sanitizeDailyPayments(loan);

  if (paymentsChanged) {
    loan.markModified("loanDetails.dailyPayment");
  }

  const rawPayments = Array.isArray(sanitizedExistingPayments)
    ? sanitizedExistingPayments
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
  const schedule = await initializeRepaymentSchedule(loan, fallbackStartDate);

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

      if (remainingAmount > 0.0001) {
        cursorDate = getNextBusinessDay(cursorDate);

        if (!cursorDate) {
          break;
        }
      }
    }
  }

  schedule.sort((first, second) => first.date - second.date);

  const maxDate = schedule.length
    ? schedule[schedule.length - 1].date
    : addDays(fallbackStartDate, WORKING_DAYS_COUNT);

  const holidayMaps = await resolveHolidayMap(fallbackStartDate, maxDate);

  schedule.forEach((entry, index) => {
    if (entry.status !== "holiday") {
      applyHolidayStatus(entry, holidayMaps);
    }

    finalizeEntryStatus(entry, index, dailyAmount);
  });

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
    loanAppForm: FORM_AMOUNT_DEFAULT,
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
    loanId: loanId || Loan.generateLoanId(),
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
    // Check if CSO has exceeded their defaulting target
    const cso = await CSO.findById(req.cso._id);
    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    const defaultingTarget = cso.defaultingTarget || 0;

    // Only check if a target is set
    if (defaultingTarget > 0) {
      // Calculate current outstanding defaults
      const loans = await Loan.find({
        csoId: req.cso._id,
        status: "active loan",
        disbursedAt: { $exists: true },
      });

      let today = getLocalToday();
      let normalizedSelectedDate = normalizeDate(today);

      if (normalizedSelectedDate.getDay() === 6) {
        normalizedSelectedDate.setDate(normalizedSelectedDate.getDate() - 1);
      } else if (normalizedSelectedDate.getDay() === 0) {
        normalizedSelectedDate.setDate(normalizedSelectedDate.getDate() - 2);
      }

      const holidays = await Holiday.find({});
      const holidaySet = new Set(
        holidays.map((h) => normalizeDate(h.holiday).getTime())
      );

      let totalOutstanding = 0;

      loans.forEach((loan) => {
        const normalizedDisbursedAt = normalizeDate(loan.disbursedAt);
        const repaymentSchedule = loan.repaymentSchedule || [];
        const scheduleCountTillToday = repaymentSchedule.filter((entry) => {
          const entryDate = normalizeDate(new Date(entry.date));
          return entryDate <= normalizedSelectedDate;
        }).length;

        const dailyAmount = loan.loanDetails?.dailyAmount || 0;
        const amountPaid = loan.loanDetails?.amountPaidSoFar || 0;
        const amountToBePaid = loan.loanDetails?.amountToBePaid || 0;

        let outstandingDue = 0;
        if (scheduleCountTillToday > 22) {
          outstandingDue = Math.max(0, amountToBePaid - amountPaid);
        } else {
          const daysElapsed = getWeekdaysBetweenSync(
            addDays(normalizedDisbursedAt, 1),
            normalizedSelectedDate,
            holidaySet
          );
          const effectiveDaysElapsed = Math.max(0, daysElapsed);
          const scheduleDaysDue = Math.max(0, scheduleCountTillToday - 1);
          const daysCounted = Math.min(effectiveDaysElapsed, scheduleDaysDue);
          const expectedRepayment = dailyAmount * daysCounted;
          outstandingDue = Math.max(0, expectedRepayment - amountPaid);
        }

        if (outstandingDue > 0.01) {
          totalOutstanding += outstandingDue;
        }
      });

      if (totalOutstanding > defaultingTarget) {
        return res.status(403).json({
          message: `Cannot create loan: Outstanding defaults (₦${totalOutstanding.toFixed(
            2
          )}) exceed your limit (₦${defaultingTarget.toFixed(2)})`,
          totalOutstanding,
          defaultingTarget,
        });
      }
    }

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

router.get("/api/admin/dashboard/analytics", async (req, res) => {
  try {
    const yearly = Number.parseInt(req.query.year, 10) || new Date().getFullYear();

    const timeframeKeys = [
      "today",
      "yesterday",
      "thisWeek",
      "thisMonth",
      "thisYear",
      "overall",
    ];

    const now = new Date();

    const timeframeRanges = timeframeKeys.reduce((acc, key) => {
      acc[key] = deriveTimeframeRange(key, now);
      return acc;
    }, {});

    const aggregation = timeframeKeys.reduce((acc, key) => {
      acc[key] = {
        loans: 0,
        disbursed: 0,
        payments: 0,
        amountToBePaid: 0,
        amountPaidSoFar: 0,
      };
      return acc;
    }, {});

    const allLoans = await Loan.find({})
      .select("disbursedAt loanDetails status")
      .lean();

    let overallAmountToBePaid = 0;
    let overallAmountPaidSoFar = 0;

    for (const loan of allLoans) {
      const disbursedAt = loan.disbursedAt ? new Date(loan.disbursedAt) : null;
      const loanDetails = loan.loanDetails || {};

      const loanAmountToBePaid = toNumber(loanDetails.amountToBePaid);
      const loanAmountPaidSoFar = toNumber(loanDetails.amountPaidSoFar);

      overallAmountToBePaid += loanAmountToBePaid;
      overallAmountPaidSoFar += loanAmountPaidSoFar;

      for (const key of timeframeKeys) {
        const range = timeframeRanges[key];

        if (disbursedAt && isWithinRange(disbursedAt, range)) {
          aggregation[key].loans += 1;
          aggregation[key].disbursed += toNumber(loanDetails.amountDisbursed);
          aggregation[key].amountToBePaid += loanAmountToBePaid;
          aggregation[key].amountPaidSoFar += loanAmountPaidSoFar;
        }

        if (Array.isArray(loanDetails.dailyPayment)) {
          for (const payment of loanDetails.dailyPayment) {
            const paymentDate = new Date(payment.date);
            if (isWithinRange(paymentDate, range)) {
              aggregation[key].payments += toNumber(payment.amount);
            }
          }
        }
      }
    }

    aggregation.overall.amountToBePaid = overallAmountToBePaid;
    aggregation.overall.amountPaidSoFar = overallAmountPaidSoFar;

    const amountToBePaid = aggregation.overall.amountToBePaid;
    const amountPaidSoFar = aggregation.overall.amountPaidSoFar;
    const loanBalance = Math.max(0, amountToBePaid - amountPaidSoFar);

    const monthlyDisbursement = await Loan.aggregate([
      {
        $match: {
          disbursedAt: {
            $gte: new Date(yearly, 0, 1),
            $lt: new Date(yearly + 1, 0, 1),
          },
        },
      },
      {
        $addFields: {
          normalizedAmountDisbursed: {
            $ifNull: [
              "$loanDetails.amountDisbursed",
              { $ifNull: ["$loanDetails.amountApproved", 0] },
            ],
          },
        },
      },
      {
        $group: {
          _id: { $month: "$disbursedAt" },
          amount: { $sum: "$normalizedAmountDisbursed" },
          count: { $sum: 1 },
        },
      },
    ]);

    const disbursementByMonth = Array.from({ length: 12 }, (_, index) => {
      const entry = monthlyDisbursement.find((item) => item._id === index + 1);
      return {
        month: new Date(0, index).toLocaleString("en", { month: "short" }),
        count: entry?.count || 0,
        amount: entry?.amount || 0,
      };
    });

    const branches = await Branch.find().select("loanTarget").lean();
    const monthlyLoanTarget = branches.reduce((sum, branch) => {
      return sum + toNumber(branch.loanTarget);
    }, 0);

    const annualLoanTarget = monthlyLoanTarget * 12;

    const chartLoanCount = aggregation.thisYear.loans;
    const loanTargetProgress = annualLoanTarget
      ? (chartLoanCount / annualLoanTarget) * 100
      : 0;

    return res.json({
      timeframe: aggregation,
      amountToBePaid,
      amountPaidSoFar,
      loanBalance,
      loanTarget: {
        annual: annualLoanTarget,
        achieved: chartLoanCount,
        progress: loanTargetProgress,
      },
      monthlyDisbursement: disbursementByMonth,
    });
  } catch (error) {
    console.error("Admin dashboard analytics error", error);
    return res.status(500).json({
      message: error.message || "Unable to compute admin dashboard analytics",
    });
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

    if (loan.status !== "waiting for approval") {
      return res
        .status(400)
        .json({ message: "Only loans waiting for approval can be approved" });
    }

    const callChecks = loan.callChecks || {};
    const pendingVerifications = [
      callChecks.callCso,
      callChecks.callCustomer,
      callChecks.callGuarantor,
      callChecks.callGroupLeader,
    ].filter((value) => !value);

    if (pendingVerifications.length > 0) {
      return res
        .status(400)
        .json({ message: "Complete all verification calls before approving this loan" });
    }

    const normalizedAmount = Number(parsedAmount.toFixed(2));
    const interestRate = await resolveInterestRate();
    const interest = Number((normalizedAmount * interestRate).toFixed(2));
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
    loan.loanDetails.interestRate = interestRate;
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

router.patch("/api/loans/:id/call-checks", async (req, res) => {
  try {
    const callChecksPayload = req.body?.callChecks || req.body || {};

    const allowedKeys = [
      "callCso",
      "callCustomer",
      "callGuarantor",
      "callGroupLeader",
    ];

    const update = {};

    allowedKeys.forEach((key) => {
      if (Object.prototype.hasOwnProperty.call(callChecksPayload, key)) {
        const value = callChecksPayload[key];
        if (typeof value !== "boolean") {
          throw new Error(`Field ${key} must be a boolean`);
        }
        update[key] = value;
      }
    });

    if (Object.keys(update).length === 0) {
      return res.status(400).json({ message: "Provide at least one call check to update" });
    }

    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    loan.callChecks = {
      callCso: loan.callChecks?.callCso || false,
      callCustomer: loan.callChecks?.callCustomer || false,
      callGuarantor: loan.callChecks?.callGuarantor || false,
      callGroupLeader: loan.callChecks?.callGroupLeader || false,
      ...update,
    };

    await loan.save();

    return res.json(loan);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to update call verification status" });
  }
});

router.get("/api/admin/loans/customer/:bvn", async (req, res) => {
  try {
    const bvnParam = req.params.bvn ? String(req.params.bvn).trim() : "";

    if (!bvnParam) {
      return res.status(400).json({ message: "Customer BVN is required" });
    }

    const loans = await Loan.find({ "customerDetails.bvn": bvnParam })
      .sort({ createdAt: -1 })
      .lean();

    return res.json(loans);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to fetch customer loans" });
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

router.post("/api/loans/:id/payments", authenticateCso, async (req, res) => {
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

    // Check if CSO has already remitted for this day
    const normalizedPaymentDate = normalizeDate(paymentDate);
    const hasRemitted = req.cso.remittance?.some((r) => {
      const remittanceDate = normalizeDate(r.date);
      return (
        remittanceDate &&
        normalizedPaymentDate &&
        remittanceDate.getTime() === normalizedPaymentDate.getTime()
      );
    });

    if (hasRemitted) {
      return res.status(400).json({
        message:
          "Cannot record payment: You have already submitted remittance for this date",
      });
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

    const {
      total: updatedPaid,
      payments: sanitizedPayments,
      changed: paymentsChanged,
    } = sanitizeDailyPayments(loan);

    if (amountToBePaid > 0 && Math.abs(updatedPaid - amountToBePaid) < 0.01) {
      loan.status = "fully paid";
    }

    if (paymentsChanged) {
      loan.loanDetails.dailyPayment = sanitizedPayments;
      loan.markModified("loanDetails.dailyPayment");
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

    const { changed: paymentsChanged } = sanitizeDailyPayments(loan);

    if (paymentsChanged) {
      await loan.save({ validateBeforeSave: false });
    }

    return res.json(loan);

    const { schedule, amountPaidSoFar } = await syncLoanRepaymentSchedule(loan);

    await loan.save({ validateBeforeSave: false });

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
    const pageParam = parseInt(req.query.page, 10);
    const limitParam = parseInt(req.query.limit, 10);
    const searchTerm =
      typeof req.query.search === "string" ? req.query.search.trim() : "";
    const groupId =
      typeof req.query.groupId === "string" ? req.query.groupId.trim() : "";

    const category =
      typeof req.query.category === "string"
        ? req.query.category.trim()
        : "all";

    const page = Number.isFinite(pageParam) && pageParam > 0 ? pageParam : 1;
    const limitRaw =
      Number.isFinite(limitParam) && limitParam > 0 ? limitParam : 16;
    const limit = Math.min(limitRaw, 100);
    const skip = (page - 1) * limit;

    const pipeline = [
      {
        $match: {
          csoId: req.cso._id,
        },
      },
    ];

    // Category Filter
    const now = new Date();
    const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    const sixtyDaysAgo = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000);

    switch (category) {
      case "active":
        pipeline.push({ $match: { status: "active loan" } });
        break;
      case "overdue":
        pipeline.push({
          $match: {
            status: "active loan",
            disbursedAt: { $lt: thirtyDaysAgo, $gte: sixtyDaysAgo },
          },
        });
        break;
      case "recovery":
        pipeline.push({
          $match: {
            status: "active loan",
            disbursedAt: { $lt: sixtyDaysAgo },
          },
        });
        break;
      case "paid":
        pipeline.push({ $match: { status: "fully paid" } });
        break;
      case "pending":
        pipeline.push({ $match: { status: "waiting for approval" } });
        break;
      case "rejected":
        pipeline.push({ $match: { status: "rejected" } });
        break;
      default:
        // "all" - no additional status match
        break;
    }

    if (groupId === "ungrouped") {
      pipeline.push({
        $match: {
          $or: [
            { "groupDetails.groupId": { $exists: false } },
            { "groupDetails.groupId": null },
            { "groupDetails.groupId": "" },
            { groupId: { $exists: false } },
            { groupId: null },
            { groupId: "" },
          ],
        },
      });
    } else if (groupId) {
      pipeline.push({
        $match: {
          $or: [{ "groupDetails.groupId": groupId }, { groupId }],
        },
      });
    }

    if (searchTerm) {
      const searchRegex = new RegExp(searchTerm, "i");
      pipeline.push({
        $match: {
          $or: [
            { "customerDetails.firstName": searchRegex },
            { "customerDetails.lastName": searchRegex },
            { "customerDetails.phoneOne": searchRegex },
            { "customerDetails.bvn": searchRegex },
            { loanId: searchRegex },
            { "businessDetails.businessName": searchRegex },
            { "businessDetails.natureOfBusiness": searchRegex },
          ],
        },
      });
    }

    pipeline.push(
      {
        $addFields: {
          normalizedBvn: {
            $let: {
              vars: {
                raw: {
                  $ifNull: ["$customerDetails.bvn", ""],
                },
              },
              in: {
                $cond: [
                  {
                    $eq: [
                      {
                        $type: "$$raw",
                      },
                      "missing",
                    ],
                  },
                  "",
                  {
                    $trim: { input: "$$raw" },
                  },
                ],
              },
            },
          },
        },
      },
      {
        $sort: { createdAt: -1 },
      },
      {
        $group: {
          _id: {
            $cond: [
              {
                $or: [
                  { $eq: ["$normalizedBvn", null] },
                  { $eq: ["$normalizedBvn", ""] },
                ],
              },
              "$_id",
              "$normalizedBvn",
            ],
          },
          doc: { $first: "$$ROOT" },
        },
      },
      {
        $replaceRoot: { newRoot: "$doc" },
      },
      {
        $project: { normalizedBvn: 0 },
      },
      {
        $sort: { createdAt: -1 },
      },
      {
        $facet: {
          data: [{ $skip: skip }, { $limit: limit }],
          totalCount: [{ $count: "count" }],
          totalBalance: [
            {
              $group: {
                _id: null,
                total: {
                  $sum: {
                    $subtract: [
                      { $ifNull: ["$loanDetails.amountToBePaid", 0] },
                      { $ifNull: ["$loanDetails.amountPaidSoFar", 0] },
                    ],
                  },
                },
              },
            },
          ],
        },
      }
    );

    const [result] = await Loan.aggregate(pipeline);

    const loans = result?.data ?? [];
    const total = result?.totalCount?.[0]?.count ?? 0;
    const totalRemainingBalance = result?.totalBalance?.[0]?.total ?? 0;
    const totalPages = total === 0 ? 1 : Math.ceil(total / limit);

    return res.json({
      loans,
      totalRemainingBalance,
      pagination: {
        page,
        limit,
        total,
        totalPages,
      },
    });
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

// Fetch admin view of loans with optional filters, pagination, and status counts
router.get("/api/admin/loans", async (req, res) => {
  try {
    const {
      status,
      search,
      csoId,
      page: pageParam,
      limit: limitParam,
    } = req.query;

    const page = Math.max(parseInt(pageParam, 10) || 1, 1);
    const limit = Math.max(parseInt(limitParam, 10) || 20, 1);
    const skip = (page - 1) * limit;

    const baseFilters = {};

    if (csoId) {
      baseFilters.csoId = csoId;
    }

    if (search && search.trim()) {
      const term = search.trim();
      const regex = new RegExp(term, "i");

      baseFilters.$or = [
        { "customerDetails.firstName": regex },
        { "customerDetails.lastName": regex },
        { "customerDetails.bvn": regex },
        { loanId: regex },
        { csoName: regex },
        { "customerDetails.businessName": regex },
      ];
    }

    const dataFilters = { ...baseFilters };

    if (status && status !== "all") {
      dataFilters.status = status;
    }

    const countFilters = { ...baseFilters };

    const [loans, totalMatching, statusBreakdown] = await Promise.all([
      Loan.find(dataFilters)
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(limit)
        .lean(),
      Loan.countDocuments(dataFilters),
      Loan.aggregate([
        { $match: countFilters },
        { $group: { _id: "$status", count: { $sum: 1 } } },
      ]),
    ]);

    const countsAccumulator = statusBreakdown.reduce(
      (acc, entry) => {
        acc.map[entry._id] = entry.count;
        acc.total += entry.count;
        return acc;
      },
      { map: {}, total: 0 }
    );

    const statusMap = countsAccumulator.map;
    const counts = {
      total: countsAccumulator.total || 0,
      active: statusMap["active loan"] || 0,
      fullyPaid: statusMap["fully paid"] || 0,
      pending: statusMap["waiting for approval"] || 0,
      rejected: statusMap.rejected || 0,
    };

    const totalPages = Math.max(Math.ceil(totalMatching / limit), 1);

    return res.json({
      data: loans,
      pagination: {
        page,
        limit,
        totalItems: totalMatching,
        totalPages,
      },
      counts,
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to load collection" });
  }
});

router.get("/api/admin/csos/:csoId/collection", async (req, res) => {
  try {
    const { csoId } = req.params;
    if (!mongoose.Types.ObjectId.isValid(csoId)) {
      return res.status(400).json({ message: "Invalid CSO identifier" });
    }

    const targetDate = normalizeDate(req.query.date || new Date());

    if (!targetDate) {
      return res.status(400).json({ message: "Invalid date supplied" });
    }

    const loans = await Loan.find({
      csoId,
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

router.get("/api/csos/loans/counts", authenticateCso, async (req, res) => {
  try {
    const counts = {
      all: 0,
      active: 0,
      defaults: 0,
      overdue: 0,
      recovery: 0,
      paid: 0,
      pending: 0,
      rejected: 0,
    };

    const loans = await Loan.find({ csoId: req.cso._id });

    const now = new Date();
    const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    const sixtyDaysAgo = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000);

    const holidays = await Holiday.find({});
    const holidaySet = new Set(
      holidays.map((h) => normalizeDate(h.holiday).getTime())
    );

    let today = getLocalToday();
    let normalizedSelectedDate = normalizeDate(today);

    if (normalizedSelectedDate.getDay() === 6) {
      normalizedSelectedDate.setDate(normalizedSelectedDate.getDate() - 1);
    } else if (normalizedSelectedDate.getDay() === 0) {
      normalizedSelectedDate.setDate(normalizedSelectedDate.getDate() - 2);
    }

    loans.forEach((loan) => {
      counts.all++;
      if (loan.status === "active loan") {
        counts.active++;
        if (loan.disbursedAt) {
          const dAt = new Date(loan.disbursedAt);
          if (dAt < thirtyDaysAgo && dAt >= sixtyDaysAgo) counts.overdue++;
          if (dAt < sixtyDaysAgo) counts.recovery++;

          // Defaults calculation
          const normalizedDisbursedAt = normalizeDate(loan.disbursedAt);
          const repaymentSchedule = loan.repaymentSchedule || [];
          const scheduleCountTillToday = repaymentSchedule.filter((entry) => {
            const entryDate = normalizeDate(new Date(entry.date));
            return entryDate <= normalizedSelectedDate;
          }).length;

          const dailyAmount = loan.loanDetails?.dailyAmount || 0;
          const amountPaid = loan.loanDetails?.amountPaidSoFar || 0;
          const amountToBePaid = loan.loanDetails?.amountToBePaid || 0;

          let outstandingDue = 0;
          if (scheduleCountTillToday > 22) {
            outstandingDue = Math.max(0, amountToBePaid - amountPaid);
          } else {
            const daysElapsed = getWeekdaysBetweenSync(
              addDays(normalizedDisbursedAt, 1),
              normalizedSelectedDate,
              holidaySet
            );
            const effectiveDaysElapsed = Math.max(0, daysElapsed);
            const scheduleDaysDue = Math.max(0, scheduleCountTillToday - 1);
            const daysCounted = Math.min(effectiveDaysElapsed, scheduleDaysDue);
            const expectedRepayment = dailyAmount * daysCounted;
            outstandingDue = Math.max(0, expectedRepayment - amountPaid);
          }

          if (outstandingDue > 0.01) {
            counts.defaults++;
          }
        }
      } else if (loan.status === "fully paid") {
        counts.paid++;
      } else if (loan.status === "waiting for approval") {
        counts.pending++;
      } else if (loan.status === "rejected") {
        counts.rejected++;
      }
    });

    return res.json(counts);
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch loan counts" });
  }
});

router.get("/api/csos/loans/outstanding", authenticateCso, async (req, res) => {
  try {
    const loans = await Loan.find({
      csoId: req.cso._id,
      status: "active loan",
      disbursedAt: { $exists: true },
    });

    let today = getLocalToday();
    let normalizedSelectedDate = normalizeDate(today);

    if (normalizedSelectedDate.getDay() === 6) {
      normalizedSelectedDate.setDate(normalizedSelectedDate.getDate() - 1);
    } else if (normalizedSelectedDate.getDay() === 0) {
      normalizedSelectedDate.setDate(normalizedSelectedDate.getDate() - 2);
    }

    const holidays = await Holiday.find({});
    const holidaySet = new Set(
      holidays.map((h) => normalizeDate(h.holiday).getTime())
    );

    const results = loans.map((loan) => {
      const disbursedAt = new Date(loan.disbursedAt);
      const normalizedDisbursedAt = normalizeDate(disbursedAt);

      const dailyAmount = loan.loanDetails?.dailyAmount || 0;
      const amountPaid = loan.loanDetails?.amountPaidSoFar || 0;
      const amountToBePaid = loan.loanDetails?.amountToBePaid || 0;
      const repaymentSchedule = loan.repaymentSchedule || [];

      const scheduleCountTillToday = repaymentSchedule.filter((entry) => {
        const entryDate = normalizeDate(new Date(entry.date));
        return entryDate <= normalizedSelectedDate;
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

    res.json({ loans: filtered, totalOutstanding });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Failed to fetch outstanding loans" });
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

router.get("/api/admin/csos/:csoId/form-collection", async (req, res) => {
  try {
    const { csoId } = req.params;
    if (!mongoose.Types.ObjectId.isValid(csoId)) {
      return res.status(400).json({ message: "Invalid CSO identifier" });
    }

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
      csoId,
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

router.get("/api/admin/transactions/disbursements", async (req, res) => {
  try {
    const rangeParam = (req.query.range || "").toLowerCase();
    const { startDate, endDate, effectiveRange } = computeAdminDateRange({
      rangeParam,
      dateParam: req.query.date,
      fromParam: req.query.from,
      toParam: req.query.to,
      fallbackYear:
        Number.parseInt(req.query.year, 10) || new Date().getFullYear(),
      fallbackMonthIndex:
        Math.min(
          Math.max(
            (Number.parseInt(req.query.month, 10) ||
              new Date().getMonth() + 1) - 1,
            0
          ),
          11
        ) || new Date().getMonth(),
    });

    const searchTerm = sanitizeSearchTerm(req.query.search);
    const csoIdParam = req.query.csoId;
    const csoObjectId = csoIdParam ? toObjectId(csoIdParam) : null;

    if (csoIdParam && !csoObjectId) {
      return res.status(400).json({ message: "Invalid CSO identifier" });
    }

    const page = Math.max(Number.parseInt(req.query.page, 10) || 1, 1);
    const limit = Math.min(
      100,
      Math.max(Number.parseInt(req.query.limit, 10) || 20, 5)
    );
    const skip = (page - 1) * limit;

    const matchStage = {
      disbursedAt: {
        $gte: startDate,
        $lt: endDate,
      },
      status: { $in: ["approved", "active loan", "fully paid"] },
    };

    if (csoObjectId) {
      matchStage.csoId = csoObjectId;
    }

    if (searchTerm) {
      const regex = new RegExp(escapeRegex(searchTerm), "i");
      matchStage.$or = [
        { "customerDetails.firstName": regex },
        { "customerDetails.lastName": regex },
        { "customerDetails.businessName": regex },
        { loanId: regex },
        { csoName: regex },
      ];
    }

    const pipeline = [
      { $match: matchStage },
      { $sort: { disbursedAt: -1 } },
      {
        $facet: {
          data: [{ $skip: skip }, { $limit: limit }],
          totals: [
            {
              $group: {
                _id: null,
                count: { $sum: 1 },
                disbursed: { $sum: "$loanDetails.amountDisbursed" },
                amountToBePaid: { $sum: "$loanDetails.amountToBePaid" },
                adminFees: { $sum: "$loanDetails.loanAppForm" },
              },
            },
          ],
        },
      },
    ];

    const [result] = await Loan.aggregate(pipeline);
    const totals = result?.totals?.[0] || {};

    const items = Array.isArray(result?.data)
      ? result.data.map(formatDisbursementEntry).filter(Boolean)
      : [];

    const totalItems = Number(totals.count || 0);
    const totalPages = totalItems > 0 ? Math.ceil(totalItems / limit) : 1;

    return res.json({
      data: items,
      pagination: {
        page,
        limit,
        totalItems,
        totalPages,
      },
      summary: {
        totalDisbursed: Number(totals.disbursed || 0),
        totalAmountToBePaid: Number(totals.amountToBePaid || 0),
        totalAdminFees: Number(totals.adminFees || 0),
      },
      filter: {
        ...formatDateFilterMeta(startDate, endDate),
        range: effectiveRange,
        search: searchTerm,
        csoId: csoIdParam || "",
      },
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to load disbursements" });
  }
});

router.get("/api/admin/transactions/collections", async (req, res) => {
  try {
    const rangeParam = (req.query.range || "").toLowerCase();
    const { startDate, endDate, effectiveRange } = computeAdminDateRange({
      rangeParam,
      dateParam: req.query.date,
      fromParam: req.query.from,
      toParam: req.query.to,
      fallbackYear:
        Number.parseInt(req.query.year, 10) || new Date().getFullYear(),
      fallbackMonthIndex:
        Math.min(
          Math.max(
            (Number.parseInt(req.query.month, 10) ||
              new Date().getMonth() + 1) - 1,
            0
          ),
          11
        ) || new Date().getMonth(),
    });

    const searchTerm = sanitizeSearchTerm(req.query.search);
    const csoIdParam = req.query.csoId;
    const csoObjectId = csoIdParam ? toObjectId(csoIdParam) : null;

    if (csoIdParam && !csoObjectId) {
      return res.status(400).json({ message: "Invalid CSO identifier" });
    }

    const page = Math.max(Number.parseInt(req.query.page, 10) || 1, 1);
    const limit = Math.min(
      100,
      Math.max(Number.parseInt(req.query.limit, 10) || 20, 5)
    );
    const skip = (page - 1) * limit;

    const matchStage = {};

    if (csoObjectId) {
      matchStage.csoId = csoObjectId;
    }

    if (searchTerm) {
      const regex = new RegExp(escapeRegex(searchTerm), "i");
      matchStage.$or = [
        { "customerDetails.firstName": regex },
        { "customerDetails.lastName": regex },
        { "customerDetails.businessName": regex },
        { loanId: regex },
        { csoName: regex },
      ];
    }

    const pipeline = [
      { $match: matchStage },
      { $unwind: "$loanDetails.dailyPayment" },
      {
        $match: {
          "loanDetails.dailyPayment.date": {
            $gte: startDate,
            $lt: endDate,
          },
        },
      },
      { $sort: { "loanDetails.dailyPayment.date": -1, createdAt: -1 } },
      {
        $facet: {
          data: [{ $skip: skip }, { $limit: limit }],
          totals: [
            {
              $group: {
                _id: null,
                count: { $sum: 1 },
                amountPaid: { $sum: "$loanDetails.dailyPayment.amount" },
              },
            },
          ],
        },
      },
    ];

    const [result] = await Loan.aggregate(pipeline);
    const totals = result?.totals?.[0] || {};

    const items = Array.isArray(result?.data)
      ? result.data
          .map((entry) =>
            formatCollectionEntry({
              ...entry,
              payment: entry.loanDetails?.dailyPayment,
            })
          )
          .filter(Boolean)
      : [];

    const totalItems = Number(totals.count || 0);
    const totalPages = totalItems > 0 ? Math.ceil(totalItems / limit) : 1;

    return res.json({
      data: items,
      pagination: {
        page,
        limit,
        totalItems,
        totalPages,
      },
      summary: {
        totalAmountPaid: Number(totals.amountPaid || 0),
      },
      filter: {
        ...formatDateFilterMeta(startDate, endDate),
        range: effectiveRange,
        search: searchTerm,
        csoId: csoIdParam || "",
      },
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to load collections" });
  }
});

router.get("/api/loans/:id", async (req, res) => {
  try {
    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    const { changed: paymentsChanged } = sanitizeDailyPayments(loan);

    if (paymentsChanged) {
      await loan.save({ validateBeforeSave: false });
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

router.patch("/api/loans/:id/request-edit", async (req, res) => {
  try {
    const { reason } = req.body || {};

    if (typeof reason !== "string" || !reason.trim()) {
      return res.status(400).json({ message: "An edit reason is required" });
    }

    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    if (loan.status === "active loan" || loan.status === "fully paid") {
      return res
        .status(400)
        .json({ message: "Cannot request edits for loans that are already disbursed" });
    }

    loan.status = "edited";
    loan.editedReason = reason.trim();
    loan.rejectionReason = undefined;

    await loan.save();

    return res.json(loan);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to request loan edit" });
  }
});

router.patch("/api/loans/:id/cso-edit", authenticateCso, async (req, res) => {
  try {
    const loan = await Loan.findById(req.params.id);

    if (!loan) {
      return res.status(404).json({ message: "Loan not found" });
    }

    if (loan.csoId.toString() !== req.cso._id.toString()) {
      return res.status(403).json({ message: "You are not authorized to edit this loan" });
    }

    if (loan.status !== "edited") {
      return res
        .status(400)
        .json({ message: "Loan is not marked for edits" });
    }

    const payload = buildLoanPayload(req.body, req.cso);

    loan.customerDetails = payload.customerDetails;
    loan.businessDetails = payload.businessDetails;
    loan.bankDetails = payload.bankDetails;
    loan.loanDetails = {
      ...loan.loanDetails,
      ...payload.loanDetails,
      amountApproved: undefined,
      interest: undefined,
      interestRate: undefined,
      amountToBePaid: payload.loanDetails?.amountToBePaid,
      dailyAmount: payload.loanDetails?.dailyAmount,
    };
    loan.guarantorDetails = payload.guarantorDetails;
    loan.groupDetails = payload.groupDetails;
    loan.pictures = payload.pictures;
    loan.status = "waiting for approval";
    loan.editedReason = undefined;
    loan.loanDetails.amountPaidSoFar = 0;
    loan.repaymentSchedule = [];

    await loan.save();

    return res.json(loan);
  } catch (error) {
    return res
      .status(400)
      .json({ message: error.message || "Unable to submit edited loan" });
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

    const startDate = loan.loanDetails?.startDate || loan.disbursedAt;
    const schedule = await initializeRepaymentSchedule(loan, startDate);

    if (!Array.isArray(loan.loanDetails.dailyPayment)) {
      loan.loanDetails.dailyPayment = [];
    }

    loan.loanDetails.amountPaidSoFar = 0;
    loan.repaymentSchedule = schedule;

    await loan.save();

    return res.json(loan);
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to disburse loan" });
  }
});

// Fetch loans submitted by authenticated CSO
// router.get("/api/loans/me", authenticateCso, async (req, res) => {
// ... (rest of the code remains the same)
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
          "csoId loanDetails.amountDisbursed loanDetails.amountToBePaid loanDetails.loanAppForm loanDetails.dailyPayment"
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
        loanAppForm: 0,
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
      metrics.loanAppForm +=
        Number(loan.loanDetails?.loanAppForm) || FORM_AMOUNT_DEFAULT;

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
      loanAppForm: Number(m.loanAppForm.toFixed(2)),
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

router.get("/api/cso-general-report", async (req, res) => {
  try {
    const now = new Date();
    const safeMonth = Number.parseInt(req.query.month, 10);
    const safeYear = Number.parseInt(req.query.year, 10);

    const month =
      Number.isFinite(safeMonth) && safeMonth >= 1 && safeMonth <= 12
        ? safeMonth
        : now.getMonth() + 1;
    const year = Number.isFinite(safeYear) ? safeYear : now.getFullYear();

    const monthStart = new Date(year, month - 1, 1);
    monthStart.setHours(0, 0, 0, 0);
    const monthEnd = new Date(year, month, 1);
    monthEnd.setHours(0, 0, 0, 0);

    const csos = await CSO.find({})
      .select(
        "firstName lastName branch branchId overdueRecords recoveryRecord"
      )
      .lean();

    const metricsMap = new Map();

    const findMonthlyRecord = (records = []) => {
      if (!Array.isArray(records)) {
        return 0;
      }

      const entry = records.find(
        (record) => record?.month === month && record?.year === year
      );
      if (!entry) {
        return 0;
      }

      const value = Number(entry.value);
      return Number.isFinite(value) ? Number(value.toFixed(2)) : 0;
    };

    for (const cso of csos) {
      const key = cso._id?.toString();
      if (!key) {
        continue;
      }

      const balanceOfDebt = findMonthlyRecord(cso.overdueRecords);
      const totalRecovery = findMonthlyRecord(cso.recoveryRecord);

      metricsMap.set(key, {
        csoId: key,
        csoName:
          `${cso.firstName || ""} ${cso.lastName || ""}`.trim() ||
          "Unnamed CSO",
        branch: cso.branch || null,
        branchId: cso.branchId || null,
        portfolioWorth: 0,
        balanceOfDebt,
        totalRepayment: 0,
        totalDisbursed: 0,
        totalInterest: 0,
        totalLoans: 0,
        totalRecovery,
        overshootValue: 0,
        tenBones: 0,
        totalLoanAppForm: 0,
        totalExpenses: 0,
        totalProfit: 0,
        loanBalance: 0,
        profitability: 0,
        loanDisbursements: [],
      });
    }

    const monthLoans = await Loan.find({
      disbursedAt: { $gte: monthStart, $lt: monthEnd },
      status: { $in: ["approved", "active loan", "fully paid"] },
    })
      .select(
        "csoId csoName disbursedAt loanDetails.amountToBePaid loanDetails.amountDisbursed loanDetails.interest loanDetails.dailyPayment"
      )
      .lean();

    for (const loan of monthLoans) {
      const csoId = loan?.csoId ? loan.csoId.toString() : null;
      if (!csoId) {
        continue;
      }

      if (!metricsMap.has(csoId)) {
        const name =
          typeof loan.csoName === "string" && loan.csoName.trim().length > 0
            ? loan.csoName.trim()
            : "Unknown CSO";
        metricsMap.set(csoId, {
          csoId,
          csoName: name,
          branch: null,
          branchId: null,
          portfolioWorth: 0,
          balanceOfDebt: 0,
          totalRepayment: 0,
          totalDisbursed: 0,
          totalInterest: 0,
          totalLoans: 0,
          totalRecovery: 0,
          overshootValue: 0,
          tenBones: 0,
          totalLoanAppForm: 0,
          totalExpenses: 0,
          totalProfit: 0,
          loanBalance: 0,
          profitability: 0,
          loanDisbursements: [],
        });
      }

      const metrics = metricsMap.get(csoId);

      const amountToBePaid = Number(loan?.loanDetails?.amountToBePaid) || 0;
      const amountDisbursed = Number(loan?.loanDetails?.amountDisbursed) || 0;
      const interestValue = Number(loan?.loanDetails?.interest);
      const loanAppFormValue = Number(loan?.loanDetails?.loanAppForm);

      metrics.totalLoans += 1;
      metrics.portfolioWorth += amountToBePaid;
      metrics.totalDisbursed += amountDisbursed;
      if (Number.isFinite(loanAppFormValue)) {
        metrics.totalLoanAppForm += loanAppFormValue;
      }

      if (Number.isFinite(interestValue)) {
        metrics.totalInterest += interestValue;
      } else if (amountToBePaid && amountDisbursed) {
        const inferredInterest = amountToBePaid - amountDisbursed;
        if (Number.isFinite(inferredInterest)) {
          metrics.totalInterest += inferredInterest;
        }
      }

      const payments = Array.isArray(loan?.loanDetails?.dailyPayment)
        ? loan.loanDetails.dailyPayment
        : [];
      for (const payment of payments) {
        const paymentDate = payment?.date ? new Date(payment.date) : null;

        if (!paymentDate || Number.isNaN(paymentDate.getTime())) {
          continue;
        }

        if (paymentDate >= monthStart && paymentDate < monthEnd) {
          metrics.totalRepayment += Number(payment.amount) || 0;
        }
      }

      const disbursedAt = loan?.disbursedAt ? new Date(loan.disbursedAt) : null;
      metrics.loanDisbursements.push({
        timestamp:
          disbursedAt && !Number.isNaN(disbursedAt.getTime())
            ? disbursedAt.getTime()
            : 0,
        amount: amountDisbursed,
      });
    }

    const monthKeyPrefix = `${year}-${String(month).padStart(2, "0")}`;
    const expensesMap = new Map();
    const reportDoc = await Report.findOne().lean();

    if (reportDoc?.expenses?.length) {
      for (const expenseEntry of reportDoc.expenses) {
        if (!expenseEntry?.date || !expenseEntry.items?.length) {
          continue;
        }

        if (!expenseEntry.date.startsWith(monthKeyPrefix)) {
          continue;
        }

        for (const item of expenseEntry.items) {
          if (!item || item.spenderType !== "cso" || !item.spenderId) {
            continue;
          }

          const amount = Number(item.amount || 0);
          if (!Number.isFinite(amount) || amount === 0) {
            continue;
          }

          const key = item.spenderId.toString();
          expensesMap.set(key, (expensesMap.get(key) || 0) + amount);
        }
      }
    }

    for (const [csoId, totalAmount] of expensesMap.entries()) {
      if (!metricsMap.has(csoId)) {
        metricsMap.set(csoId, {
          csoId,
          csoName: "Unknown CSO",
          branch: null,
          branchId: null,
          portfolioWorth: 0,
          balanceOfDebt: 0,
          totalRepayment: 0,
          totalDisbursed: 0,
          totalInterest: 0,
          totalLoans: 0,
          totalRecovery: 0,
          overshootValue: 0,
          tenBones: 0,
          totalLoanAppForm: 0,
          totalExpenses: 0,
          totalProfit: 0,
          loanBalance: 0,
          profitability: 0,
          loanDisbursements: [],
        });
      }

      const metrics = metricsMap.get(csoId);
      metrics.totalExpenses += totalAmount;
    }

    const data = [];
    const summary = {
      totalCsos: metricsMap.size,
      portfolioWorth: 0,
      balanceOfDebt: 0,
      totalRepayment: 0,
      totalDisbursed: 0,
      totalInterest: 0,
      totalLoans: 0,
      totalRecovery: 0,
      overshootValue: 0,
      tenBones: 0,
      totalLoanAppForm: 0,
      totalExpenses: 0,
      totalProfit: 0,
      loanBalance: 0,
    };

    for (const metrics of metricsMap.values()) {
      if (metrics.loanDisbursements.length > 100) {
        metrics.loanDisbursements.sort(
          (first, second) => first.timestamp - second.timestamp
        );
        const overshootCount = metrics.loanDisbursements.length - 100;
        const overshootEntries = metrics.loanDisbursements.slice(
          -overshootCount
        );
        metrics.overshootValue = overshootEntries.reduce(
          (acc, entry) => acc + (Number(entry.amount) || 0),
          0
        );
      }

      metrics.tenBones = Number((metrics.overshootValue * 0.01).toFixed(2));
      metrics.totalProfit = Number(
        (
          metrics.totalInterest +
          metrics.totalLoanAppForm -
          metrics.totalExpenses
        ).toFixed(2)
      );
      metrics.loanBalance = Number(
        Math.max(0, metrics.portfolioWorth - metrics.totalRepayment).toFixed(2)
      );

      const profitBase = metrics.totalInterest + metrics.totalLoanAppForm;
      metrics.profitability =
        profitBase > 0
          ? Number(((metrics.totalProfit / profitBase) * 100).toFixed(2))
          : 0;

      const output = {
        csoId: metrics.csoId,
        csoName: metrics.csoName,
        branch: metrics.branch,
        branchId: metrics.branchId,
        portfolioWorth: Number(metrics.portfolioWorth.toFixed(2)),
        balanceOfDebt: Number(metrics.balanceOfDebt.toFixed(2)),
        totalRepayment: Number(metrics.totalRepayment.toFixed(2)),
        totalDisbursed: Number(metrics.totalDisbursed.toFixed(2)),
        totalInterest: Number(metrics.totalInterest.toFixed(2)),
        totalLoans: metrics.totalLoans,
        totalRecovery: Number(metrics.totalRecovery.toFixed(2)),
        overshootValue: Number(metrics.overshootValue.toFixed(2)),
        tenBones: Number(metrics.tenBones.toFixed(2)),
        totalLoanAppForm: Number(metrics.totalLoanAppForm.toFixed(2)),
        totalExpenses: Number(metrics.totalExpenses.toFixed(2)),
        totalProfit: metrics.totalProfit,
        loanBalance: metrics.loanBalance,
        profitability: metrics.profitability,
      };

      summary.portfolioWorth += output.portfolioWorth;
      summary.balanceOfDebt += output.balanceOfDebt;
      summary.totalRepayment += output.totalRepayment;
      summary.totalDisbursed += output.totalDisbursed;
      summary.totalInterest += output.totalInterest;
      summary.totalLoans += output.totalLoans;
      summary.totalRecovery += output.totalRecovery;
      summary.overshootValue += output.overshootValue;
      summary.tenBones += output.tenBones;
      summary.totalLoanAppForm += output.totalLoanAppForm;
      summary.totalExpenses += output.totalExpenses;
      summary.totalProfit += output.totalProfit;
      summary.loanBalance += output.loanBalance;

      data.push(output);
    }

    data.sort((first, second) => first.csoName.localeCompare(second.csoName));

    summary.portfolioWorth = Number(summary.portfolioWorth.toFixed(2));
    summary.balanceOfDebt = Number(summary.balanceOfDebt.toFixed(2));
    summary.totalRepayment = Number(summary.totalRepayment.toFixed(2));
    summary.totalDisbursed = Number(summary.totalDisbursed.toFixed(2));
    summary.totalInterest = Number(summary.totalInterest.toFixed(2));
    summary.totalRecovery = Number(summary.totalRecovery.toFixed(2));
    summary.overshootValue = Number(summary.overshootValue.toFixed(2));
    summary.tenBones = Number(summary.tenBones.toFixed(2));
    summary.totalLoanAppForm = Number(summary.totalLoanAppForm.toFixed(2));
    summary.totalExpenses = Number(summary.totalExpenses.toFixed(2));
    summary.totalProfit = Number(summary.totalProfit.toFixed(2));
    summary.loanBalance = Number(summary.loanBalance.toFixed(2));

    const monthTimeline = await Loan.aggregate([
      {
        $match: {
          status: { $in: ["approved", "active loan", "fully paid"] },
          disbursedAt: { $ne: null },
        },
      },
      {
        $group: {
          _id: {
            year: { $year: "$disbursedAt" },
            month: { $month: "$disbursedAt" },
          },
        },
      },
      { $sort: { "_id.year": -1, "_id.month": -1 } },
      { $limit: 36 },
    ]);

    const availableMonths = monthTimeline
      .map((entry) => {
        const entryYear = entry?._id?.year;
        const entryMonth = entry?._id?.month;

        if (!entryYear || !entryMonth) {
          return null;
        }

        const labelDate = new Date(entryYear, entryMonth - 1, 1);
        return {
          year: entryYear,
          month: entryMonth,
          label: labelDate.toLocaleString(undefined, {
            month: "short",
            year: "numeric",
          }),
        };
      })
      .filter(Boolean);

    return res.json({
      month: { year, month },
      data,
      summary,
      availableMonths,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    return res.status(500).json({
      message: error.message || "Unable to compute CSO general report",
    });
  }
});

router.get("/api/cso-weekly-loan-counts", async (req, res) => {
  try {
    const now = new Date();
    const monthParam = Number.parseInt(req.query.month, 10);
    const yearParam = Number.parseInt(req.query.year, 10);

    const safeMonth =
      monthParam >= 1 && monthParam <= 12 ? monthParam : now.getMonth() + 1;
    const safeYear = Number.isFinite(yearParam) ? yearParam : now.getFullYear();

    const monthStart = new Date(safeYear, safeMonth - 1, 1);
    monthStart.setHours(0, 0, 0, 0);
    const monthEnd = new Date(safeYear, safeMonth, 1);
    monthEnd.setHours(0, 0, 0, 0);

    const weekDefinitions = buildWeekDefinitions(safeYear, safeMonth);
    const weekTotals = createWeekTemplate(weekDefinitions);

    const loans = await Loan.find({
      status: { $in: ACTIVE_LOAN_STATUSES },
      disbursedAt: { $gte: monthStart, $lt: monthEnd },
    })
      .select("csoId csoName disbursedAt")
      .lean();

    const csoMap = new Map();

    const ensureCsoEntry = (loan) => {
      const key = loan?.csoId ? loan.csoId.toString() : null;

      if (!key) {
        return null;
      }

      if (!csoMap.has(key)) {
        const name =
          typeof loan.csoName === "string" && loan.csoName.trim().length > 0
            ? loan.csoName.trim()
            : "Unknown CSO";

        csoMap.set(key, {
          csoId: key,
          csoName: name,
          weeks: createWeekTemplate(weekDefinitions),
          total: 0,
        });
      }

      return csoMap.get(key);
    };

    for (const loan of loans) {
      const entry = ensureCsoEntry(loan);

      if (!entry) {
        continue;
      }

      const disbursedAt = loan?.disbursedAt ? new Date(loan.disbursedAt) : null;

      if (!disbursedAt || Number.isNaN(disbursedAt.getTime())) {
        continue;
      }

      const dayOfMonth = disbursedAt.getDate();
      const weekIndex = Math.max(
        0,
        Math.min(weekDefinitions.length - 1, Math.floor((dayOfMonth - 1) / 7))
      );

      const week = entry.weeks[weekIndex];

      if (!week) {
        continue;
      }

      week.count += 1;
      entry.total += 1;
      weekTotals[weekIndex].count += 1;
    }

    const data = Array.from(csoMap.values()).sort((a, b) =>
      a.csoName.localeCompare(b.csoName)
    );

    const monthTimeline = await Loan.aggregate([
      {
        $match: {
          status: { $in: ACTIVE_LOAN_STATUSES },
          disbursedAt: { $ne: null },
        },
      },
      {
        $group: {
          _id: {
            year: { $year: "$disbursedAt" },
            month: { $month: "$disbursedAt" },
          },
        },
      },
      { $sort: { "_id.year": -1, "_id.month": -1 } },
      { $limit: 36 },
    ]);

    const availableMonths = monthTimeline
      .map((entry) => {
        const year = entry?._id?.year;
        const month = entry?._id?.month;

        if (!year || !month) {
          return null;
        }

        const labelDate = new Date(year, month - 1, 1);
        return {
          year,
          month,
          label: labelDate.toLocaleString(undefined, {
            month: "short",
            year: "numeric",
          }),
        };
      })
      .filter(Boolean);

    return res.json({
      month: {
        year: safeYear,
        month: safeMonth,
      },
      data,
      weeks: weekDefinitions.map((definition, index) => ({
        index: definition.index,
        label: definition.label,
        startDate: definition.startIso,
        endDate: definition.endIso,
        total: weekTotals[index]?.count || 0,
      })),
      summary: {
        totalCsos: data.length,
        totalLoans: loans.length,
        weekTotals: weekTotals.map((week) => ({
          index: week.index,
          label: week.label,
          startDate: week.startDate,
          endDate: week.endDate,
          count: week.count,
        })),
      },
      availableMonths,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    return res.status(500).json({
      message: error.message || "Unable to compute CSO weekly loan counts",
    });
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
    const pageParam = Number.parseInt(req.query.page, 10);
    const limitParam = Number.parseInt(req.query.limit, 10);

    const page = Number.isFinite(pageParam) && pageParam > 0 ? pageParam : 1;
    const limitBase =
      Number.isFinite(limitParam) && limitParam > 0 ? limitParam : 20;
    const limit = Math.min(limitBase, 100);

    const cso = await CSO.findById(csoId)
      .select("firstName lastName branch branchId signature")
      .lean();

    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    const csoClauses = [{ csoId }];
    const fullName = `${cso.firstName || ""} ${cso.lastName || ""}`.trim();

    if (fullName) {
      csoClauses.push({
        csoName: new RegExp(`^${escapeRegex(fullName)}$`, "i"),
      });
    }

    const filter = {
      status: { $in: ["active loan", "fully paid", "approved"] },
    };

    const andClauses = [];
    andClauses.push({ $or: csoClauses });

    if (groupId) {
      if (groupId === "ungrouped") {
        andClauses.push({
          $or: [
            { "groupDetails.groupId": { $exists: false } },
            { "groupDetails.groupId": null },
            { "groupDetails.groupId": "" },
          ],
        });
      } else {
        andClauses.push({ "groupDetails.groupId": groupId });
      }
    }

    if (search && search.trim()) {
      const trimmed = search.trim();
      const searchRegex = new RegExp(escapeRegex(trimmed), "i");
      andClauses.push({
        $or: [
          { "customerDetails.firstName": searchRegex },
          { "customerDetails.lastName": searchRegex },
          { "customerDetails.bvn": searchRegex },
          { "customerDetails.phoneOne": searchRegex },
          { loanId: searchRegex },
        ],
      });
    }

    if (andClauses.length > 0) {
      filter.$and = andClauses;
    }

    const total = await Loan.countDocuments(filter);

    const safePage = Math.min(page, Math.max(1, Math.ceil(total / limit) || 1));
    const skip = (safePage - 1) * limit;

    const loans = await Loan.find(filter)
      .skip(skip)
      .limit(limit)
      .select(
        "loanId customerDetails loanDetails groupDetails status csoId csoName branch branchId"
      )
      .sort({ "customerDetails.firstName": 1 })
      .lean();

    const formatted = loans.map((loan) => {
      const requiresCsoAssignment = String(loan.csoId || "") !== String(csoId);

      return {
        ...loan,
        requiresCsoAssignment,
      };
    });

    return res.json({
      customers: formatted,
      cso: {
        _id: csoId,
        name: fullName || null,
        branch: cso.branch || null,
        branchId: cso.branchId || null,
        signature: cso.signature || null,
      },
      pagination: {
        page: safePage,
        limit,
        total,
        totalPages: Math.max(1, Math.ceil(total / limit) || 1),
      },
    });
  } catch (error) {
    return res
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

router.post("/api/loans/assign-cso", async (req, res) => {
  try {
    const { loanIds, csoId } = req.body;

    if (!Array.isArray(loanIds) || loanIds.length === 0) {
      return res.status(400).json({ message: "No customers selected" });
    }

    if (!csoId) {
      return res.status(400).json({ message: "CSO is required" });
    }

    const cso = await CSO.findById(csoId)
      .select("firstName lastName branch branchId signature")
      .lean();

    if (!cso) {
      return res.status(404).json({ message: "CSO not found" });
    }

    const fullName = `${cso.firstName || ""} ${cso.lastName || ""}`.trim();

    const updatePayload = {
      csoId: cso._id,
      csoName: fullName || null,
      branch: cso.branch || null,
      branchId: cso.branchId || null,
    };

    if (cso.signature) {
      updatePayload.csoSignature = cso.signature;
    }

    const result = await Loan.updateMany(
      { _id: { $in: loanIds } },
      { $set: updatePayload }
    );

    return res.json({
      message: "Customers reassigned to CSO successfully",
      modifiedCount: result.modifiedCount || 0,
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to assign customers to CSO" });
  }
});

module.exports = router;
