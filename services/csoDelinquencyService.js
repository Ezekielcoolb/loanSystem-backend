const mongoose = require("mongoose");
const CSO = require("../models/cso");
const Loan = require("../models/loan");

const ACTIVE_LOAN_STATUSES = ["active loan", "approved"];
const MIN_OUTSTANDING_THRESHOLD = 0.5;

function normalizeDate(value) {
  const date = value instanceof Date ? new Date(value) : new Date(value);

  if (Number.isNaN(date.getTime())) {
    return null;
  }

  date.setHours(0, 0, 0, 0);
  return date;
}

function toCurrencyNumber(value) {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return 0;
  }

  return Number(number.toFixed(2));
}

function differenceInDays(laterDate, earlierDate) {
  if (!(laterDate instanceof Date) || !(earlierDate instanceof Date)) {
    return 0;
  }

  const laterTime = laterDate.getTime();
  const earlierTime = earlierDate.getTime();

  if (!Number.isFinite(laterTime) || !Number.isFinite(earlierTime)) {
    return 0;
  }

  const diffMs = laterTime - earlierTime;
  return Math.floor(diffMs / (1000 * 60 * 60 * 24));
}

function getEarliestDelinquentDate(schedule, asOfDate) {
  if (!Array.isArray(schedule) || schedule.length === 0) {
    return null;
  }

  const normalizedAsOf = normalizeDate(asOfDate);
  if (!normalizedAsOf) {
    return null;
  }

  let earliest = null;

  for (const entry of schedule) {
    if (!entry || !entry.date) {
      continue;
    }

    const status = typeof entry.status === "string" ? entry.status.toLowerCase() : "";
    if (status && status !== "pending" && status !== "partial" && status !== "approved") {
      continue;
    }

    const entryDate = normalizeDate(entry.date);
    if (!entryDate) {
      continue;
    }

    if (entryDate > normalizedAsOf) {
      continue;
    }

    if (!earliest || entryDate < earliest) {
      earliest = entryDate;
    }
  }

  return earliest;
}

async function computeDelinquencyMap(asOfDate) {
  const normalizedAsOf = normalizeDate(asOfDate) || new Date();
  const loans = await Loan.find({
    status: { $in: ACTIVE_LOAN_STATUSES },
    disbursedAt: { $ne: null },
    csoId: { $ne: null },
  })
    .select(
      "csoId repaymentSchedule disbursedAt loanDetails.amountToBePaid loanDetails.amountPaidSoFar loanDetails.balance loanDetails.amountDisbursed"
    )
    .lean();

  const delinquencyMap = new Map();

  for (const loan of loans) {
    const csoId = loan?.csoId ? loan.csoId.toString() : null;
    if (!csoId) {
      continue;
    }

    const amountToBePaid = toCurrencyNumber(loan?.loanDetails?.amountToBePaid);
    const amountPaidSoFar = toCurrencyNumber(loan?.loanDetails?.amountPaidSoFar);
    const balance = Math.max(0, amountToBePaid - amountPaidSoFar);

    if (balance < MIN_OUTSTANDING_THRESHOLD) {
      continue;
    }

    const delinquentDate =
      getEarliestDelinquentDate(loan?.repaymentSchedule, normalizedAsOf) || normalizeDate(loan?.disbursedAt);

    if (!delinquentDate) {
      continue;
    }

    const daysPastDue = differenceInDays(normalizedAsOf, delinquentDate);

    if (daysPastDue < 30) {
      continue;
    }

    const entry = delinquencyMap.get(csoId) || { overdueValue: 0, recoveryValue: 0 };

    if (daysPastDue >= 60) {
      entry.recoveryValue += balance;
    } else {
      entry.overdueValue += balance;
    }

    delinquencyMap.set(csoId, entry);
  }

  for (const [key, value] of delinquencyMap.entries()) {
    value.overdueValue = toCurrencyNumber(value.overdueValue);
    value.recoveryValue = toCurrencyNumber(value.recoveryValue);
  }

  return delinquencyMap;
}

async function replaceMonthlyRecord(session, csoId, field, year, month, value) {
  const normalizedValue = toCurrencyNumber(value);
  const now = new Date();

  await CSO.updateOne(
    { _id: csoId },
    { $pull: { [field]: { year, month } } },
    { session }
  );

  await CSO.updateOne(
    { _id: csoId },
    {
      $push: {
        [field]: {
          year,
          month,
          value: normalizedValue,
          updatedAt: now,
        },
      },
    },
    { session }
  );
}

async function updateCsoDelinquencyRecords({ asOfDate = new Date(), includeInactive = false } = {}) {
  const normalizedAsOf = normalizeDate(asOfDate) || new Date();
  const month = normalizedAsOf.getMonth() + 1;
  const year = normalizedAsOf.getFullYear();

  const delinquencyMap = await computeDelinquencyMap(normalizedAsOf);
  const csoFilter = includeInactive ? {} : { isActive: true };
  const csos = await CSO.find(csoFilter).select("_id").lean();

  const summary = {
    processed: 0,
    updated: 0,
    errors: [],
    month,
    year,
    asOf: normalizedAsOf.toISOString(),
  };

  for (const cso of csos) {
    const csoId = cso._id;
    const mapEntry = delinquencyMap.get(csoId.toString()) || { overdueValue: 0, recoveryValue: 0 };
    let session;

    try {
      session = await mongoose.startSession();
      await session.withTransaction(async () => {
        await replaceMonthlyRecord(session, csoId, "overdueRecords", year, month, mapEntry.overdueValue);
        await replaceMonthlyRecord(session, csoId, "recoveryRecord", year, month, mapEntry.recoveryValue);
      });
      summary.updated += 1;
    } catch (error) {
      summary.errors.push({ csoId: csoId.toString(), message: error.message });
    } finally {
      summary.processed += 1;
      if (session) {
        session.endSession();
      }
    }
  }

  return summary;
}

module.exports = {
  updateCsoDelinquencyRecords,
  computeDelinquencyMap,
};
