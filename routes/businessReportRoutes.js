const express = require("express");
const Loan = require("../models/loan");
const Report = require("../models/Report");
const CSO = require("../models/cso");

const router = express.Router();

const ACTIVE_LOAN_STATUSES = ["approved", "active loan", "fully paid"];

function normalizeStartOfDay(date) {
  const normalized = new Date(date);
  normalized.setHours(0, 0, 0, 0);
  return normalized;
}

function normalizeEndOfDay(date) {
  const normalized = new Date(date);
  normalized.setHours(23, 59, 59, 999);
  return normalized;
}

function addDays(date, days) {
  const result = new Date(date);
  result.setDate(result.getDate() + days);
  return result;
}

function startOfWeek(date) {
  const normalized = normalizeStartOfDay(date);
  const day = normalized.getDay();
  const diff = (day + 6) % 7; // Monday as start of week
  normalized.setDate(normalized.getDate() - diff);
  return normalized;
}

function formatDateKey(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatDisplay(date) {
  return date.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
  });
}

function formatLabel(start, end) {
  const startLabel = formatDisplay(start);
  const endLabel = formatDisplay(end);
  if (startLabel === endLabel) {
    return startLabel;
  }
  return `${startLabel} - ${endLabel}`;
}

const DAY_LABEL_FORMAT = {
  weekday: "short",
  day: "numeric",
  month: "short",
};

function clampDateToRange(date, min, max) {
  if (date < min) {
    return new Date(min.getTime());
  }

  if (date > max) {
    return new Date(max.getTime());
  }

  return date;
}

function buildBusinessDays(rangeStart, rangeEnd, monthStart, monthEnd) {
  const days = [];
  const map = new Map();

  for (let offset = 0; offset < 5; offset += 1) {
    const candidate = addDays(rangeStart, offset);
    if (candidate > rangeEnd) {
      break;
    }

    const clampedStart = clampDateToRange(candidate, monthStart, monthEnd);
    const dayStart = normalizeStartOfDay(clampedStart);
    const dayEnd = normalizeEndOfDay(
      clampDateToRange(candidate, monthStart, monthEnd)
    );

    if (dayStart > monthEnd || dayEnd < monthStart) {
      break;
    }

    const dayKey = formatDateKey(dayStart);

    const label = dayStart.toLocaleDateString("en-GB", DAY_LABEL_FORMAT);

    const entry = {
      order: offset,
      date: dayKey,
      label,
      start: dayStart,
      end: dayEnd,
      loanCount: 0,
      totalDisbursed: 0,
      totalLoanAppForm: 0,
      totalInterest: 0,
      totalExpenses: 0,
      profit: 0,
      loanBalance: 0,
      cashAtHand: 0,
      growth: 0,
    };

    days.push(entry);
    map.set(dayKey, entry);
  }

  return { days, map };
}

function parseDateKey(value) {
  if (typeof value !== "string") {
    return null;
  }
  const [yearStr, monthStr, dayStr] = value.split("-");
  const year = Number.parseInt(yearStr, 10);
  const month = Number.parseInt(monthStr, 10);
  const day = Number.parseInt(dayStr, 10);
  if (
    !Number.isFinite(year) ||
    !Number.isFinite(month) ||
    !Number.isFinite(day)
  ) {
    return null;
  }
  return new Date(year, month - 1, day);
}

function buildWeekSegments(month, year) {
  const monthStart = normalizeStartOfDay(new Date(year, month - 1, 1));
  const monthEnd = normalizeEndOfDay(new Date(year, month, 0));

  const segments = [];
  let cursor = startOfWeek(monthStart);
  let order = 0;

  while (cursor <= monthEnd) {
    const weekStart = normalizeStartOfDay(cursor);
    const rawWeekEnd = addDays(weekStart, 4);

    const clampedStart = new Date(
      Math.max(weekStart.getTime(), monthStart.getTime())
    );
    const clampedEnd = new Date(
      Math.min(rawWeekEnd.getTime(), monthEnd.getTime())
    );

    const rangeStart = normalizeStartOfDay(clampedStart);
    const rangeEnd = normalizeEndOfDay(clampedEnd);

    if (rangeStart <= rangeEnd) {
      segments.push({
        order,
        weekStart,
        rangeStart,
        rangeEnd,
        weekKey: formatDateKey(weekStart),
        label: formatLabel(rangeStart, rangeEnd),
      });
      order += 1;
    }

    cursor = addDays(weekStart, 7);
  }

  return { segments, monthStart, monthEnd };
}

function findWeekForDate(date, segments) {
  for (const segment of segments) {
    if (date >= segment.rangeStart && date <= segment.rangeEnd) {
      return segment;
    }
  }
  return null;
}

function sumExpenseItems(items = []) {
  return items.reduce((total, item) => total + Number(item.amount || 0), 0);
}

router.get("/api/business-report/weekly-metrics", async (req, res) => {
  try {
    const now = new Date();
    const safeMonth = Number.parseInt(req.query.month, 10);
    const safeYear = Number.parseInt(req.query.year, 10);

    const month =
      Number.isFinite(safeMonth) && safeMonth >= 1 && safeMonth <= 12
        ? safeMonth
        : now.getMonth() + 1;
    const year = Number.isFinite(safeYear) ? safeYear : now.getFullYear();

    const { segments, monthStart, monthEnd } = buildWeekSegments(month, year);

    if (segments.length === 0) {
      return res.json({ month: { month, year }, weeks: [] });
    }

    const nextMonthStart = normalizeStartOfDay(addDays(monthEnd, 1));

    const weeklyMetrics = segments.map((segment) => {
      const { days, map } = buildBusinessDays(
        segment.rangeStart,
        segment.rangeEnd,
        monthStart,
        monthEnd
      );

      return {
        order: segment.order,
        weekKey: segment.weekKey,
        label: segment.label,
        rangeStart: segment.rangeStart,
        rangeEnd: segment.rangeEnd,
        startDate: formatDateKey(segment.rangeStart),
        endDate: formatDateKey(segment.rangeEnd),
        loanCount: 0,
        totalDisbursed: 0,
        totalLoanAppForm: 0,
        totalInterest: 0,
        totalExpenses: 0,
        days,
        _dayMap: map,
      };
    });

    const monthlyLoans = await Loan.find({
      $or: [
        { disbursedAt: { $gte: monthStart, $lt: nextMonthStart } },
        { createdAt: { $gte: monthStart, $lt: nextMonthStart } },
      ],
    })
      .select(
        "disbursedAt createdAt loanDetails.amountDisbursed loanDetails.loanAppForm loanDetails.interest loanDetails.amountToBePaid"
      )
      .lean();

    for (const loan of monthlyLoans) {
      const disbursedAt = loan?.disbursedAt ? new Date(loan.disbursedAt) : null;
      const createdAt = loan?.createdAt ? new Date(loan.createdAt) : null;

      const loanAppForm = Number(loan?.loanDetails?.loanAppForm || 0);
      const amountDisbursed = Number(loan?.loanDetails?.amountDisbursed || 0);
      const amountToBePaid = Number(loan?.loanDetails?.amountToBePaid || 0);
      let interest = Number(loan?.loanDetails?.interest);

      if (!Number.isFinite(interest)) {
        if (
          Number.isFinite(amountToBePaid) &&
          Number.isFinite(amountDisbursed)
        ) {
          interest = amountToBePaid - amountDisbursed;
        } else {
          interest = 0;
        }
      }

      // 1. Process Form Fees by CreatedAt
      if (createdAt && createdAt >= monthStart && createdAt < nextMonthStart) {
        const weekday = createdAt.getDay();
        if (weekday !== 0 && weekday !== 6) {
          const week = findWeekForDate(createdAt, segments);
          if (week) {
            const weekData = weeklyMetrics[week.order];
            if (weekData && Number.isFinite(loanAppForm)) {
              weekData.totalLoanAppForm += loanAppForm;
              const dayKey = formatDateKey(normalizeStartOfDay(createdAt));
              const dayEntry = weekData._dayMap.get(dayKey);
              if (dayEntry) {
                dayEntry.totalLoanAppForm += loanAppForm;
              }
            }
          }
        }
      }

      // 2. Process Disbursements by DisbursedAt
      if (
        disbursedAt &&
        disbursedAt >= monthStart &&
        disbursedAt < nextMonthStart
      ) {
        const weekday = disbursedAt.getDay();
        if (weekday !== 0 && weekday !== 6) {
          const week = findWeekForDate(disbursedAt, segments);
          if (week) {
            const weekData = weeklyMetrics[week.order];
            if (weekData) {
              const dayKey = formatDateKey(normalizeStartOfDay(disbursedAt));
              const dayEntry = weekData._dayMap.get(dayKey);

              weekData.loanCount += 1;
              if (Number.isFinite(amountDisbursed)) {
                weekData.totalDisbursed += amountDisbursed;
                if (dayEntry) {
                  dayEntry.totalDisbursed += amountDisbursed;
                }
              }

              if (Number.isFinite(interest)) {
                weekData.totalInterest += interest;
                if (dayEntry) {
                  dayEntry.totalInterest += interest;
                }
              }

              if (dayEntry) {
                dayEntry.loanCount += 1;
              }
            }
          }
        }
      }
    }

    const report = await Report.findOne().lean();

    if (report?.expenses?.length) {
      for (const entry of report.expenses) {
        if (!entry?.date) {
          continue;
        }
        const entryDate = parseDateKey(entry.date);
        if (!entryDate || entryDate < monthStart || entryDate > monthEnd) {
          continue;
        }
        const weekday = entryDate.getDay();
        if (weekday === 0 || weekday === 6) {
          continue;
        }
        const expenseTotal = sumExpenseItems(entry.items);
        if (!expenseTotal) {
          continue;
        }
        const week = findWeekForDate(entryDate, segments);
        if (!week) {
          continue;
        }
        const weekData = weeklyMetrics[week.order];
        if (!weekData) {
          continue;
        }
        weekData.totalExpenses += expenseTotal;
        const dayKey = formatDateKey(normalizeStartOfDay(entryDate));
        const dayEntry = weekData._dayMap.get(dayKey);
        if (dayEntry) {
          dayEntry.totalExpenses += expenseTotal;
        }
      }
    }

    const weeksResponse = weeklyMetrics.map((week) => {
      const profitRaw =
        week.totalInterest + week.totalLoanAppForm - week.totalExpenses;

      const days = week.days.map((day) => {
        const dayProfit =
          day.totalInterest + day.totalLoanAppForm - day.totalExpenses;

        return {
          order: day.order,
          date: day.date,
          label: day.label,
          loanCount: day.loanCount,
          totalDisbursed: Number(day.totalDisbursed.toFixed(2)),
          totalLoanAppForm: Number(day.totalLoanAppForm.toFixed(2)),
          totalInterest: Number(day.totalInterest.toFixed(2)),
          totalExpenses: Number(day.totalExpenses.toFixed(2)),
          profit: Number(dayProfit.toFixed(2)),
        };
      });

      delete week._dayMap;

      return {
        weekKey: week.weekKey,
        order: week.order,
        label: week.label,
        startDate: week.startDate,
        endDate: week.endDate,
        loanCount: week.loanCount,
        totalDisbursed: Number(week.totalDisbursed.toFixed(2)),
        totalLoanAppForm: Number(week.totalLoanAppForm.toFixed(2)),
        totalInterest: Number(week.totalInterest.toFixed(2)),
        totalExpenses: Number(week.totalExpenses.toFixed(2)),
        profit: Number(profitRaw.toFixed(2)),
        days,
      };
    });

    return res.json({ month: { month, year }, weeks: weeksResponse });
  } catch (error) {
    return res.status(500).json({
      message: error.message || "Unable to compute business weekly metrics",
    });
  }
});

router.get("/api/business-report/liquidity", async (req, res) => {
  try {
    const now = new Date();
    const safeMonth = Number.parseInt(req.query.month, 10);
    const safeYear = Number.parseInt(req.query.year, 10);

    const month =
      Number.isFinite(safeMonth) && safeMonth >= 1 && safeMonth <= 12
        ? safeMonth
        : now.getMonth() + 1;
    const year = Number.isFinite(safeYear) ? safeYear : now.getFullYear();

    const { segments, monthStart, monthEnd } = buildWeekSegments(month, year);

    if (segments.length === 0) {
      return res.json({ month: { month, year }, weeks: [] });
    }

    const periodStart = segments[0].rangeStart;
    const periodEnd = segments[segments.length - 1].rangeEnd;
    const nextMonthStart = normalizeStartOfDay(addDays(monthEnd, 1));

    const liquidityWeeks = segments.map((segment) => {
      const { days } = buildBusinessDays(
        segment.rangeStart,
        segment.rangeEnd,
        monthStart,
        monthEnd
      );

      return {
        order: segment.order,
        weekKey: segment.weekKey,
        label: segment.label,
        rangeStart: segment.rangeStart,
        rangeEnd: segment.rangeEnd,
        startDate: formatDateKey(segment.rangeStart),
        endDate: formatDateKey(segment.rangeEnd),
        loanBalance: 0,
        cashAtHand: 0,
        growth: 0,
        days,
      };
    });

    const rawLoans = await Loan.find({
      disbursedAt: { $lt: nextMonthStart },
    })
      .select("disbursedAt loanDetails.amountToBePaid loanDetails.dailyPayment")
      .lean();

    const processedLoans = rawLoans
      .map((loan) => {
        const amountToBePaid = Number(loan?.loanDetails?.amountToBePaid || 0);
        if (!Number.isFinite(amountToBePaid) || amountToBePaid <= 0) {
          return null;
        }

        const disbursedAt = loan?.disbursedAt
          ? normalizeStartOfDay(new Date(loan.disbursedAt))
          : null;

        if (!disbursedAt || Number.isNaN(disbursedAt.getTime())) {
          return null;
        }

        const paymentsRaw = Array.isArray(loan?.loanDetails?.dailyPayment)
          ? loan.loanDetails.dailyPayment
          : [];

        const seenIds = new Set();
        const payments = [];

        for (const payment of paymentsRaw) {
          const amount = Number(payment?.amount ?? payment?.amountPaid ?? 0);
          if (!Number.isFinite(amount) || amount <= 0) {
            continue;
          }

          const paymentDate = payment?.date
            ? normalizeStartOfDay(new Date(payment.date))
            : null;

          if (!paymentDate || Number.isNaN(paymentDate.getTime())) {
            continue;
          }

          const paymentId = payment?._id ? payment._id.toString() : null;
          if (paymentId) {
            if (seenIds.has(paymentId)) {
              continue;
            }
            seenIds.add(paymentId);
          }

          payments.push({ date: paymentDate, amount });
        }

        payments.sort((first, second) => first.date - second.date);

        let runningPaid = 0;
        let paymentIdx = 0;

        while (
          paymentIdx < payments.length &&
          payments[paymentIdx].date < periodStart
        ) {
          runningPaid += payments[paymentIdx].amount;
          paymentIdx += 1;
        }

        runningPaid = Math.min(runningPaid, amountToBePaid);

        return {
          amountToBePaid,
          disbursedDate: disbursedAt,
          payments,
          paymentIdx,
          runningPaid,
        };
      })
      .filter(Boolean);

    const report = await Report.findOne().lean();

    const cashSnapshots = Array.isArray(report?.cashAtHand)
      ? report.cashAtHand
          .map((entry) => ({
            date: parseDateKey(entry.date),
            amount: Number(entry.amount || 0),
          }))
          .filter((entry) => entry.date && entry.date <= monthEnd)
          .sort((first, second) => first.date - second.date)
      : [];

    const cashMap = new Map();
    for (const snapshot of cashSnapshots) {
      if (snapshot.date) {
        cashMap.set(formatDateKey(snapshot.date), snapshot.amount);
      }
    }

    const allDays = [];
    for (const week of liquidityWeeks) {
      week.days.sort((first, second) => first.start - second.start);
      for (const day of week.days) {
        allDays.push({ day, week });
      }
    }

    allDays.sort((first, second) => first.day.start - second.day.start);

    for (const { day } of allDays) {
      if (day.start > periodEnd) {
        day.loanBalance = 0;
        day.cashAtHand = Number((cashMap.get(day.date) || 0).toFixed(2));
        day.growth = Number(day.cashAtHand.toFixed(2));
        continue;
      }

      let totalBalance = 0;

      for (const loan of processedLoans) {
        if (!loan.disbursedDate || loan.disbursedDate > day.end) {
          continue;
        }

        while (
          loan.paymentIdx < loan.payments.length &&
          loan.payments[loan.paymentIdx].date <= day.end
        ) {
          loan.runningPaid += loan.payments[loan.paymentIdx].amount;
          loan.paymentIdx += 1;
        }

        const cappedPaid = Math.min(loan.runningPaid, loan.amountToBePaid);
        const balance = loan.amountToBePaid - cappedPaid;

        if (balance > 0) {
          totalBalance += balance;
        }
      }

      day.loanBalance = Number(totalBalance.toFixed(2));

      const cashValue = Number(cashMap.get(day.date) || 0);
      day.cashAtHand = Number(cashValue.toFixed(2));
      day.growth = Number((day.loanBalance + day.cashAtHand).toFixed(2));
    }

    let snapshotIndex = 0;
    let currentCash = 0;

    for (const week of liquidityWeeks) {
      const sortedDays = week.days.slice().sort((a, b) => a.end - b.end);
      const lastDay = sortedDays[sortedDays.length - 1] || null;
      const weekBalance = lastDay ? lastDay.loanBalance : 0;

      while (
        snapshotIndex < cashSnapshots.length &&
        cashSnapshots[snapshotIndex].date <= week.rangeEnd
      ) {
        currentCash = cashSnapshots[snapshotIndex].amount;
        snapshotIndex += 1;
      }

      const normalizedCash = Number((currentCash || 0).toFixed(2));

      week.loanBalance = Number(weekBalance.toFixed(2));
      week.cashAtHand = normalizedCash;
      week.growth = Number((week.loanBalance + normalizedCash).toFixed(2));
    }

    const weeksResponse = liquidityWeeks.map((week) => ({
      weekKey: week.weekKey,
      order: week.order,
      label: week.label,
      startDate: week.startDate,
      endDate: week.endDate,
      loanBalance: week.loanBalance,
      cashAtHand: week.cashAtHand,
      growth: week.growth,
      days: week.days.map((day) => ({
        order: day.order,
        date: day.date,
        label: day.label,
        loanBalance: Number(day.loanBalance.toFixed(2)),
        cashAtHand: Number(day.cashAtHand.toFixed(2)),
        growth: Number(day.growth.toFixed(2)),
      })),
    }));

    return res.json({ month: { month, year }, weeks: weeksResponse });
  } catch (error) {
    return res.status(500).json({
      message: error.message || "Unable to compute liquidity snapshot",
    });
  }
});

router.get("/api/business-report/monthly-summary", async (req, res) => {
  try {
    const safeYear = Number.parseInt(req.query.year, 10);
    const year = Number.isFinite(safeYear)
      ? safeYear
      : new Date().getFullYear();

    const startOfYear = new Date(year, 0, 1, 0, 0, 0, 0);
    const endOfYear = new Date(year, 11, 31, 23, 59, 59, 999);

    // 1. Loan Aggregation (Monthly Metrics)
    // We need metrics per month: Loan Count, Disbursed, Repayment, Interest, Loan Application Form.
    // Also we need "Disbursed" and "Paid" to calculate Running Balance.

    const [loanMetrics, chartExpense, csoMetrics, loansForBalance] =
      await Promise.all([
        Loan.aggregate([
          {
            $facet: {
              disbursements: [
                {
                  $match: {
                    disbursedAt: { $gte: startOfYear, $lte: endOfYear },
                  },
                },
                {
                  $group: {
                    _id: { $month: "$disbursedAt" }, // 1-12
                    count: { $sum: 1 },
                    amountDisbursed: {
                      $sum: { $toDouble: "$loanDetails.amountDisbursed" },
                    },
                    totalDebtDisbursed: {
                      $sum: { $toDouble: "$loanDetails.amountToBePaid" },
                    },
                    totalInterest: {
                      $sum: { $toDouble: "$loanDetails.interest" },
                    },
                    totalForm: {
                      $sum: { $toDouble: "$loanDetails.loanAppForm" },
                    },
                  },
                },
              ],
              payments: [
                {
                  $match: {
                    "loanDetails.dailyPayment.date": {
                      $gte: startOfYear,
                      $lte: endOfYear,
                    },
                  },
                },
                { $unwind: "$loanDetails.dailyPayment" },
                {
                  $match: {
                    "loanDetails.dailyPayment.date": {
                      $gte: startOfYear,
                      $lte: endOfYear,
                    },
                  },
                },
                {
                  $group: {
                    _id: { $month: "$loanDetails.dailyPayment.date" }, // 1-12
                    totalRepayment: {
                      $sum: {
                        $toDouble: {
                          $ifNull: [
                            "$loanDetails.dailyPayment.amount",
                            "$loanDetails.dailyPayment.amountPaid",
                            0,
                          ],
                        },
                      },
                    },
                  },
                },
              ],
            },
          },
        ]),

        // Expenses aggregation
        Report.aggregate([
          { $unwind: "$expenses" },
          { $unwind: "$expenses.items" },
          {
            $project: {
              date: { $toDate: "$expenses.date" },
              amount: { $toDouble: "$expenses.items.amount" },
            },
          },
          {
            $match: {
              date: { $gte: startOfYear, $lte: endOfYear },
            },
          },
          {
            $group: {
              _id: { $month: "$date" },
              totalExpenses: { $sum: "$amount" },
            },
          },
        ]),

        // CSO Metrics (Overdue, Recovery)
        CSO.aggregate([
          {
            $facet: {
              overdue: [
                { $unwind: "$overdueRecords" },
                {
                  $match: {
                    "overdueRecords.year": year,
                  },
                },
                {
                  $group: {
                    _id: "$overdueRecords.month",
                    total: { $sum: "$overdueRecords.value" },
                  },
                },
              ],
              recovery: [
                { $unwind: "$recoveryRecord" },
                {
                  $match: {
                    "recoveryRecord.year": year,
                  },
                },
                {
                  $group: {
                    _id: "$recoveryRecord.month",
                    total: { $sum: "$recoveryRecord.value" },
                  },
                },
              ],
            },
          },
        ]),

        Loan.find({
          $or: [
            { disbursedAt: { $lte: endOfYear } },
            {
              "loanDetails.dailyPayment.date": {
                $gte: startOfYear,
                $lte: endOfYear,
              },
            },
          ],
        })
          .select(
            "disbursedAt loanDetails.amountToBePaid loanDetails.dailyPayment"
          )
          .lean(),
      ]);

    // Process Basic Metrics Maps
    const disbursementMap = new Map(); // month -> {count, amount, interest, form}
    loanMetrics[0].disbursements.forEach((d) => {
      disbursementMap.set(d._id, d);
    });

    const repaymentMap = new Map(); // month -> totalRepayment
    loanMetrics[0].payments.forEach((p) => {
      repaymentMap.set(p._id, p.totalRepayment);
    });

    const expenseMap = new Map(); // month -> totalExpenses
    chartExpense.forEach((e) => {
      expenseMap.set(e._id, e.totalExpenses);
    });

    const overdueMap = new Map(); // month -> total
    if (csoMetrics[0]?.overdue) {
      csoMetrics[0].overdue.forEach((o) => overdueMap.set(o._id, o.total));
    }

    const recoveryMap = new Map(); // month -> total
    if (csoMetrics[0]?.recovery) {
      csoMetrics[0].recovery.forEach((r) => recoveryMap.set(r._id, r.total));
    }

    // Process Cash At Hand (Get last value for each month)
    const reportDoc = await Report.findOne().lean();
    const cashEvents = Array.isArray(reportDoc?.cashAtHand)
      ? reportDoc.cashAtHand
          .map((c) => ({
            date: new Date(c.date),
            amount: Number(c.amount),
          }))
          .sort((a, b) => a.date - b.date)
      : [];

    const cashMap = new Map(); // month -> last known cash amount
    // We need to iterate month by month to carry over last known cash if no update in a month?
    // User requested "last cash at hand in the month".
    // If a month has no entry, it technically has the last cash from previous month.
    // Let's resolve this during the month loop.

    // Process Loan Balance with cumulative disbursement/payment logic
    const monthEnds = Array.from(
      { length: 12 },
      (_, index) => new Date(year, index + 1, 0, 23, 59, 59, 999)
    );

    const monthlyBalanceTotals = Array(12).fill(0);

    for (const loan of loansForBalance) {
      const loanDetails = loan.loanDetails || {};
      const amountToBePaid = Number(loanDetails.amountToBePaid) || 0;
      if (amountToBePaid <= 0) {
        continue;
      }

      const disbursedRaw = loan.disbursedAt ? new Date(loan.disbursedAt) : null;
      const disbursedAt =
        disbursedRaw && !Number.isNaN(disbursedRaw.getTime())
          ? disbursedRaw
          : null;

      const payments = Array.isArray(loanDetails.dailyPayment)
        ? loanDetails.dailyPayment
        : [];

      const uniquePayments = [];
      const seenIds = new Set();

      payments.forEach((payment, index) => {
        if (!payment || typeof payment !== "object") {
          return;
        }

        const paymentDateRaw = payment.date ? new Date(payment.date) : null;
        if (!paymentDateRaw || Number.isNaN(paymentDateRaw.getTime())) {
          return;
        }

        const paymentAmount = Number(payment.amount ?? payment.amountPaid ?? 0);
        if (!Number.isFinite(paymentAmount) || paymentAmount <= 0) {
          return;
        }

        const paymentId = payment._id
          ? payment._id.toString()
          : `${paymentDateRaw.getTime()}-${paymentAmount}-${index}`;

        if (seenIds.has(paymentId)) {
          return;
        }

        seenIds.add(paymentId);
        uniquePayments.push({ date: paymentDateRaw, amount: paymentAmount });
      });

      uniquePayments.sort((first, second) => first.date - second.date);

      let paymentPointer = 0;
      let cumulativePaid = 0;

      monthEnds.forEach((monthEnd, idx) => {
        if (disbursedAt && disbursedAt > monthEnd) {
          return;
        }

        while (
          paymentPointer < uniquePayments.length &&
          uniquePayments[paymentPointer].date <= monthEnd
        ) {
          cumulativePaid += uniquePayments[paymentPointer].amount;
          paymentPointer += 1;
        }

        const cappedPaid = Math.min(cumulativePaid, amountToBePaid);
        const remaining = amountToBePaid - cappedPaid;

        if (remaining > 0) {
          monthlyBalanceTotals[idx] += remaining;
        }
      });
    }

    const monthlyData = [];

    for (let m = 1; m <= 12; m++) {
      // Metrics
      const disbData = disbursementMap.get(m) || {};
      const loanCount = disbData.count || 0;
      const amountDisbursed = disbData.amountDisbursed || 0; // Principal
      const totalDebtDisbursed = disbData.totalDebtDisbursed || 0; // Total Debt (Principal + Interest)
      const totalInterest = disbData.totalInterest || 0;
      const totalLoanAppForm = disbData.totalForm || 0;

      const totalRepayment = repaymentMap.get(m) || 0;
      const totalExpenses = expenseMap.get(m) || 0;

      const totalOverdue = overdueMap.get(m) || 0;
      const totalRecovery = recoveryMap.get(m) || 0;

      // Profit
      const totalProfit = totalInterest + totalLoanAppForm - totalExpenses;

      const loanBalance = Number((monthlyBalanceTotals[m - 1] || 0).toFixed(2));

      // Cash At Hand (Find last entry <= End of Month m)
      const monthEndDate = new Date(year, m, 0); // Last day of month m
      // Find last cash event <= monthEndDate
      let lastCash = 0;
      // We can optimize by keeping a cursor if needed, but array search is fine for standard report size.
      // Search from end or filter?
      // Since it's cumulative, we want the *latest* status.
      // Filter events <= monthEndDate, take last.
      const validCash = cashEvents.filter((c) => c.date <= monthEndDate);
      if (validCash.length > 0) {
        lastCash = validCash[validCash.length - 1].amount;
      }

      // Growth
      const growth = loanBalance + lastCash;

      monthlyData.push({
        month: m,
        year,
        loanCount,
        amountDisbursed: Number(amountDisbursed.toFixed(2)),
        totalRepayment: Number(totalRepayment.toFixed(2)),
        totalInterest: Number(totalInterest.toFixed(2)),
        totalLoanAppForm: Number(totalLoanAppForm.toFixed(2)),
        totalExpenses: Number(totalExpenses.toFixed(2)),
        totalProfit: Number(totalProfit.toFixed(2)),
        totalOverdue: Number(totalOverdue.toFixed(2)),
        totalRecovery: Number(totalRecovery.toFixed(2)),
        lastCashAtHand: Number.isFinite(lastCash)
          ? Number(lastCash.toFixed(2))
          : 0,
        loanBalance: Number.isFinite(loanBalance) ? loanBalance : 0,
        growth: Number.isFinite(growth) ? Number(growth.toFixed(2)) : 0,
      });
    }

    return res.json({ year, monthSummary: monthlyData });
  } catch (error) {
    console.error("Monthly Summary Error:", error);
    return res
      .status(500)
      .json({ connectionError: "Unable to generate report" });
  }
});

module.exports = router;
