const express = require("express");
const mongoose = require("mongoose");
const Branch = require("../models/branch");
const Loan = require("../models/loan");
const CSO = require("../models/cso");
const Report = require("../models/Report");

const router = express.Router();

function parseNumeric(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

async function distributeTargetsToCsos(branchDoc, { loanTarget, disbursementTarget }) {
  const shouldUpdateLoan = Number.isFinite(loanTarget);
  const shouldUpdateDisbursement = Number.isFinite(disbursementTarget);

  if (!shouldUpdateLoan && !shouldUpdateDisbursement) {
    return;
  }

  const branchId = branchDoc._id.toString();
  const csos = await CSO.find({ branchId }).select("_id");

  if (!csos.length) {
    return;
  }

  const perCsoLoanTarget = shouldUpdateLoan ? Math.round(loanTarget / csos.length) : null;
  const perCsoDisbursementTarget = shouldUpdateDisbursement
    ? Number((disbursementTarget / csos.length).toFixed(2))
    : null;

  const operations = csos
    .map((cso) => {
      const $set = {};

      if (perCsoLoanTarget !== null) {
        $set.loanTarget = perCsoLoanTarget;
      }

      if (perCsoDisbursementTarget !== null) {
        $set.disbursementTarget = perCsoDisbursementTarget;
      }

      if (!Object.keys($set).length) {
        return null;
      }

      return {
        updateOne: {
          filter: { _id: cso._id },
          update: { $set },
        },
      };
    })
    .filter(Boolean);

  if (operations.length) {
    await CSO.bulkWrite(operations);
  }
}

async function applyTargetsToBranch(branchDoc, targets) {
  const updates = {};

  if (Number.isFinite(targets.loanTarget)) {
    const roundedLoanTarget = Math.round(targets.loanTarget);
    branchDoc.loanTarget = roundedLoanTarget;
    updates.loanTarget = roundedLoanTarget;
  }

  if (Number.isFinite(targets.disbursementTarget)) {
    branchDoc.disbursementTarget = targets.disbursementTarget;
    updates.disbursementTarget = targets.disbursementTarget;
  }

  if (Object.keys(updates).length) {
    await branchDoc.save();
    await distributeTargetsToCsos(branchDoc, updates);
  }

  return branchDoc;
}

// Create a new branch
router.post("/api/branches", async (req, res) => {
  try {
    const branch = await Branch.create(req.body);
    return res.status(201).json(branch);
  } catch (error) {
    if (error.code === 11000) {
      return res.status(409).json({ message: "Supervisor email already exists" });
    }
    return res.status(400).json({ message: error.message || "Unable to create branch" });
  }
});

// Retrieve all branches
router.get("/api/branches", async (_req, res) => {
  try {
    const branches = await Branch.find().sort({ createdAt: -1 });
    return res.json(branches);
  } catch (error) {
    return res.status(500).json({ message: "Unable to fetch branches" });
  }
});

// Delete a branch
router.delete("/api/branches/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const deleted = await Branch.findByIdAndDelete(id);

    if (!deleted) {
      return res.status(404).json({ message: "Branch not found" });
    }

    return res.json({ message: "Branch deleted" });
  } catch (error) {
    return res.status(500).json({ message: "Unable to delete branch" });
  }
});

// Update branch targets
router.patch("/api/branches/:id/targets", async (req, res) => {
  try {
    const { id } = req.params;
    const loanTarget = parseNumeric(req.body.loanTarget);
    const disbursementTarget = parseNumeric(req.body.disbursementTarget);

    if (!Number.isFinite(loanTarget) && !Number.isFinite(disbursementTarget)) {
      return res.status(400).json({ message: "Provide at least one numeric target" });
    }

    if (id === "all") {
      const branches = await Branch.find();

      if (!branches.length) {
        return res.status(404).json({ message: "No branches found" });
      }

      const updatedBranches = [];
      for (const branch of branches) {
        const updated = await applyTargetsToBranch(branch, { loanTarget, disbursementTarget });
        updatedBranches.push(updated);
      }

      return res.json(updatedBranches);
    }

    if (!mongoose.Types.ObjectId.isValid(id)) {
      return res.status(400).json({ message: "Invalid branch identifier" });
    }

    const branch = await Branch.findById(id);

    if (!branch) {
      return res.status(404).json({ message: "Branch not found" });
    }

    const updatedBranch = await applyTargetsToBranch(branch, { loanTarget, disbursementTarget });

    return res.json(updatedBranch);
  } catch (error) {
    return res.status(400).json({ message: error.message || "Unable to update branch targets" });
  }
});

router.get("/api/branches/:id/metrics", async (req, res) => {
  try {
    const { id } = req.params;

    if (!mongoose.Types.ObjectId.isValid(id)) {
      return res.status(400).json({ message: "Invalid branch identifier" });
    }

    const branch = await Branch.findById(id)
      .select("name loanTarget disbursementTarget createdAt")
      .lean();

    if (!branch) {
      return res.status(404).json({ message: "Branch not found" });
    }

    const now = new Date();
    const monthParam = Number.parseInt(req.query.month, 10);
    const yearParam = Number.parseInt(req.query.year, 10);

    const safeMonth = monthParam >= 1 && monthParam <= 12 ? monthParam : now.getMonth() + 1;
    const safeYear = Number.isFinite(yearParam) ? yearParam : now.getFullYear();

    const rangeStart = new Date(safeYear, safeMonth - 1, 1);
    rangeStart.setHours(0, 0, 0, 0);
    const rangeEnd = new Date(safeYear, safeMonth, 1);
    rangeEnd.setHours(0, 0, 0, 0);

    const branchIdString = branch._id.toString();
    const branchName = typeof branch.name === "string" ? branch.name.trim() : null;

    const branchFilters = [{ branchId: branchIdString }];
    if (branchIdString !== id) {
      branchFilters.push({ branchId: id });
    }
    if (branchName) {
      branchFilters.push({ branch: branchName });
    }

    const eligibleStatuses = ["approved", "active loan", "fully paid"];

    const baseMatch = {
      status: { $in: eligibleStatuses },
      disbursedAt: { $ne: null },
      $or: branchFilters,
    };

    const [aggregateResult] = await Loan.aggregate([
      { $match: baseMatch },
      {
        $facet: {
          lifetime: [
            {
              $group: {
                _id: null,
                loanCount: { $sum: 1 },
                totalDisbursed: {
                  $sum: { $ifNull: ["$loanDetails.amountDisbursed", 0] },
                },
                totalAmountToBePaid: {
                  $sum: { $ifNull: ["$loanDetails.amountToBePaid", 0] },
                },
                totalAdminFees: {
                  $sum: { $ifNull: ["$loanDetails.loanAppForm", 0] },
                },
              },
            },
          ],
          lifetimePayments: [
            { $unwind: "$loanDetails.dailyPayment" },
            {
              $group: {
                _id: null,
                totalAmountPaid: {
                  $sum: { $ifNull: ["$loanDetails.dailyPayment.amount", 0] },
                },
              },
            },
          ],
          monthDisbursements: [
            { $match: { disbursedAt: { $gte: rangeStart, $lt: rangeEnd } } },
            {
              $group: {
                _id: null,
                loanCount: { $sum: 1 },
                totalDisbursed: {
                  $sum: { $ifNull: ["$loanDetails.amountDisbursed", 0] },
                },
                totalAmountToBePaid: {
                  $sum: { $ifNull: ["$loanDetails.amountToBePaid", 0] },
                },
                totalAdminFees: {
                  $sum: { $ifNull: ["$loanDetails.loanAppForm", 0] },
                },
              },
            },
          ],
          monthPayments: [
            { $unwind: "$loanDetails.dailyPayment" },
            {
              $match: {
                "loanDetails.dailyPayment.date": { $gte: rangeStart, $lt: rangeEnd },
              },
            },
            {
              $group: {
                _id: null,
                totalAmountPaid: {
                  $sum: { $ifNull: ["$loanDetails.dailyPayment.amount", 0] },
                },
              },
            },
          ],
          disbursementTimeline: [
            {
              $group: {
                _id: {
                  year: { $year: "$disbursedAt" },
                  month: { $month: "$disbursedAt" },
                },
                loanCount: { $sum: 1 },
                totalDisbursed: {
                  $sum: { $ifNull: ["$loanDetails.amountDisbursed", 0] },
                },
                totalAmountToBePaid: {
                  $sum: { $ifNull: ["$loanDetails.amountToBePaid", 0] },
                },
                totalAdminFees: {
                  $sum: { $ifNull: ["$loanDetails.loanAppForm", 0] },
                },
              },
            },
            { $sort: { "_id.year": -1, "_id.month": -1 } },
          ],
          paymentTimeline: [
            { $unwind: "$loanDetails.dailyPayment" },
            {
              $group: {
                _id: {
                  year: { $year: "$loanDetails.dailyPayment.date" },
                  month: { $month: "$loanDetails.dailyPayment.date" },
                },
                totalAmountPaid: {
                  $sum: { $ifNull: ["$loanDetails.dailyPayment.amount", 0] },
                },
              },
            },
            { $sort: { "_id.year": -1, "_id.month": -1 } },
          ],
        },
      },
    ]);

    const lifetimeTotals = aggregateResult?.lifetime?.[0] || {};
    const lifetimePaid = aggregateResult?.lifetimePayments?.[0]?.totalAmountPaid || 0;

    const lifetimeDisbursed = lifetimeTotals.totalDisbursed || 0;
    const lifetimeAmountToBePaid = lifetimeTotals.totalAmountToBePaid || 0;
    const lifetimeAdminFees = lifetimeTotals.totalAdminFees || 0;
    const lifetimeInterest = lifetimeAmountToBePaid - lifetimeDisbursed;
    const lifetimeOutstanding = lifetimeAmountToBePaid - lifetimePaid;
    const lifetimeProfit = lifetimeInterest + lifetimeAdminFees;

    const monthDisbursements = aggregateResult?.monthDisbursements?.[0] || {};
    const monthPaid = aggregateResult?.monthPayments?.[0]?.totalAmountPaid || 0;

    const monthDisbursed = monthDisbursements.totalDisbursed || 0;
    const monthAmountToBePaid = monthDisbursements.totalAmountToBePaid || 0;
    const monthAdminFees = monthDisbursements.totalAdminFees || 0;
    const monthInterest = monthAmountToBePaid - monthDisbursed;
    const monthProfit = monthInterest + monthAdminFees;
    const monthBalanceGap = monthAmountToBePaid - monthPaid;

    const paymentTimelineMap = new Map();
    for (const paymentEntry of aggregateResult?.paymentTimeline || []) {
      if (!paymentEntry?._id?.year || !paymentEntry?._id?.month) {
        continue;
      }

      const key = `${paymentEntry._id.year}-${String(paymentEntry._id.month).padStart(2, "0")}`;
      paymentTimelineMap.set(key, paymentEntry.totalAmountPaid || 0);
    }

    const combinedTimeline = [];
    const seenKeys = new Set();

    for (const disbursementEntry of aggregateResult?.disbursementTimeline || []) {
      if (!disbursementEntry?._id?.year || !disbursementEntry?._id?.month) {
        continue;
      }

      const key = `${disbursementEntry._id.year}-${String(disbursementEntry._id.month).padStart(2, "0")}`;
      seenKeys.add(key);

      combinedTimeline.push({
        year: disbursementEntry._id.year,
        month: disbursementEntry._id.month,
        loanCount: disbursementEntry.loanCount || 0,
        totalDisbursed: disbursementEntry.totalDisbursed || 0,
        totalAmountToBePaid: disbursementEntry.totalAmountToBePaid || 0,
        totalAdminFees: disbursementEntry.totalAdminFees || 0,
        totalAmountPaid: paymentTimelineMap.get(key) || 0,
      });
    }

    for (const [key, amountPaid] of paymentTimelineMap.entries()) {
      if (seenKeys.has(key)) {
        continue;
      }

      const [yearString, monthString] = key.split("-");
      const numericYear = Number.parseInt(yearString, 10);
      const numericMonth = Number.parseInt(monthString, 10);

      combinedTimeline.push({
        year: numericYear,
        month: numericMonth,
        loanCount: 0,
        totalDisbursed: 0,
        totalAmountToBePaid: 0,
        totalAdminFees: 0,
        totalAmountPaid: amountPaid || 0,
      });
    }

    combinedTimeline.sort((first, second) => {
      if (first.year === second.year) {
        return second.month - first.month;
      }
      return second.year - first.year;
    });

    const availableMonths = combinedTimeline.slice(0, 24).map((entry) => {
      const labelDate = new Date(entry.year, entry.month - 1, 1);
      return {
        year: entry.year,
        month: entry.month,
        label: labelDate.toLocaleString(undefined, {
          month: "short",
          year: "numeric",
        }),
        loanCount: entry.loanCount,
        totalDisbursed: entry.totalDisbursed,
        totalAmountPaid: entry.totalAmountPaid,
        totalAmountToBePaid: entry.totalAmountToBePaid,
        totalAdminFees: entry.totalAdminFees,
      };
    });

    const responsePayload = {
      branch: {
        id: branch._id,
        name: branch.name,
        loanTarget: branch.loanTarget ?? 0,
        disbursementTarget: branch.disbursementTarget ?? 0,
        createdAt: branch.createdAt,
      },
      lifetime: {
        loanCount: lifetimeTotals.loanCount || 0,
        totalDisbursed: lifetimeDisbursed,
        totalAmountToBePaid: lifetimeAmountToBePaid,
        totalAdminFees: lifetimeAdminFees,
        totalInterest: lifetimeInterest,
        totalAmountPaid: lifetimePaid,
        outstandingBalance: lifetimeOutstanding,
        totalProfit: lifetimeProfit,
      },
      month: {
        month: safeMonth,
        year: safeYear,
        loanCount: monthDisbursements.loanCount || 0,
        totalDisbursed: monthDisbursed,
        totalAmountToBePaid: monthAmountToBePaid,
        totalAdminFees: monthAdminFees,
        totalInterest: monthInterest,
        totalAmountPaid: monthPaid,
        totalProfit: monthProfit,
        balanceGap: monthBalanceGap,
        range: {
          start: rangeStart.toISOString(),
          end: new Date(rangeEnd.getTime() - 1).toISOString(),
        },
      },
      availableMonths,
      generatedAt: new Date().toISOString(),
    };

    return res.json(responsePayload);
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to compute branch metrics" });
  }
});

router.get("/api/branches/:id/cso-metrics", async (req, res) => {
  try {
    const { id } = req.params;
    const rawSearch = typeof req.query.search === "string" ? req.query.search.trim() : "";

    if (!mongoose.Types.ObjectId.isValid(id)) {
      return res.status(400).json({ message: "Invalid branch identifier" });
    }

    const branch = await Branch.findById(id).select("name").lean();

    if (!branch) {
      return res.status(404).json({ message: "Branch not found" });
    }

    const now = new Date();
    const monthParam = Number.parseInt(req.query.month, 10);
    const yearParam = Number.parseInt(req.query.year, 10);

    const safeMonth = monthParam >= 1 && monthParam <= 12 ? monthParam : now.getMonth() + 1;
    const safeYear = Number.isFinite(yearParam) ? yearParam : now.getFullYear();

    const monthStart = new Date(safeYear, safeMonth - 1, 1);
    monthStart.setHours(0, 0, 0, 0);
    const monthEnd = new Date(safeYear, safeMonth, 1);
    monthEnd.setHours(0, 0, 0, 0);

    const branchIdString = branch._id.toString();
    const branchName = typeof branch.name === "string" ? branch.name.trim() : null;

    const branchFilters = [{ branchId: branchIdString }];
    if (branchIdString !== id) {
      branchFilters.push({ branchId: id });
    }
    if (branchName) {
      branchFilters.push({ branch: branchName });
    }

    const baseMatch = {
      status: { $in: ["approved", "active loan", "fully paid"] },
      disbursedAt: { $ne: null },
      $or: branchFilters,
    };

    const [csos, monthLoans, cumulativeLoans, paymentsThisMonth, cumulativePayments, expensesReport, monthTimeline] = await Promise.all([
      CSO.find({
        isActive: true,
        $or: [
          { branchId: branchIdString },
          { branchId: id },
          branchName ? { branch: branchName } : null,
        ].filter(Boolean),
      })
        .select("firstName lastName loanTarget disbursementTarget branchId")
        .lean(),
      Loan.find({
        ...baseMatch,
        disbursedAt: { $gte: monthStart, $lt: monthEnd },
        status: { $in: ["active loan", "fully paid"] },
      })
        .select(
          "csoId csoName loanDetails.amountDisbursed loanDetails.amountToBePaid loanDetails.loanAppForm"
        )
        .lean(),
      Loan.find({
        ...baseMatch,
        disbursedAt: { $lt: monthEnd },
        status: { $in: ["active loan", "fully paid"] },
      })
        .select("csoId csoName loanDetails.amountToBePaid")
        .lean(),
      Loan.aggregate([
        { $match: { ...baseMatch } },
        { $unwind: "$loanDetails.dailyPayment" },
        {
          $match: {
            "loanDetails.dailyPayment.date": { $gte: monthStart, $lt: monthEnd },
          },
        },
        {
          $group: {
            _id: "$csoId",
            totalAmount: {
              $sum: { $ifNull: ["$loanDetails.dailyPayment.amount", 0] },
            },
          },
        },
      ]),
      Loan.aggregate([
        { $match: { ...baseMatch } },
        { $unwind: "$loanDetails.dailyPayment" },
        {
          $match: {
            "loanDetails.dailyPayment.date": { $lt: monthEnd },
          },
        },
        {
          $group: {
            _id: "$csoId",
            totalAmount: {
              $sum: { $ifNull: ["$loanDetails.dailyPayment.amount", 0] },
            },
          },
        },
      ]),
      Report.findOne().lean(),
      Loan.aggregate([
        { $match: { ...baseMatch } },
        {
          $group: {
            _id: {
              year: { $year: "$disbursedAt" },
              month: { $month: "$disbursedAt" },
            },
          },
        },
        { $sort: { "_id.year": -1, "_id.month": -1 } },
        { $limit: 24 },
      ]),
    ]);

    const metricsMap = new Map();

    const ensureMetric = (csoId, details = {}) => {
      const key = csoId?.toString();
      if (!key) {
        return null;
      }

      if (!metricsMap.has(key)) {
        metricsMap.set(key, {
          csoId: key,
          csoName: details.csoName || "Unknown CSO",
          loanTarget: Number(details.loanTarget || 0),
          disbursementTarget: Number(details.disbursementTarget || 0),
          loansThisMonth: 0,
          totalDisbursed: 0,
          amountToBePaid: 0,
          interest: 0,
          adminFee: 0,
          amountPaid: 0,
          expenses: 0,
          cumulativeAmountToBePaid: 0,
          cumulativePayments: 0,
        });
      }

      return metricsMap.get(key);
    };

    for (const cso of csos) {
      const csoIdStr = cso._id?.toString();
      if (!csoIdStr) continue;

      ensureMetric(csoIdStr, {
        csoName: `${cso.firstName || ""} ${cso.lastName || ""}`.trim() || "Unknown CSO",
        loanTarget: cso.loanTarget,
        disbursementTarget: cso.disbursementTarget,
      });
    }

    for (const loan of monthLoans) {
      const metric = ensureMetric(loan.csoId, {
        csoName: loan.csoName,
      });

      if (!metric) continue;

      const amountDisbursed = Number(loan.loanDetails?.amountDisbursed || 0);
      const amountToBePaid = Number(loan.loanDetails?.amountToBePaid || 0);
      const adminFee = Number(loan.loanDetails?.loanAppForm || 0);

      metric.loansThisMonth += 1;
      metric.totalDisbursed += amountDisbursed;
      metric.amountToBePaid += amountToBePaid;
      metric.adminFee += adminFee;
      metric.interest += amountToBePaid - amountDisbursed;
    }

    for (const loan of cumulativeLoans) {
      const metric = ensureMetric(loan.csoId, {
        csoName: loan.csoName,
      });

      if (!metric) continue;

      const amountToBePaid = Number(loan.loanDetails?.amountToBePaid || 0);
      metric.cumulativeAmountToBePaid += amountToBePaid;
    }

    for (const entry of paymentsThisMonth) {
      const metric = ensureMetric(entry._id, {});
      if (!metric) continue;
      metric.amountPaid += Number(entry.totalAmount || 0);
    }

    for (const entry of cumulativePayments) {
      const metric = ensureMetric(entry._id, {});
      if (!metric) continue;
      metric.cumulativePayments += Number(entry.totalAmount || 0);
    }

    if (expensesReport?.expenses?.length) {
      const monthKeyPrefix = `${safeYear}-${String(safeMonth).padStart(2, "0")}`;
      for (const expenseEntry of expensesReport.expenses) {
        if (!expenseEntry?.date || !expenseEntry.items?.length) continue;
        if (!expenseEntry.date.startsWith(monthKeyPrefix)) continue;

        for (const item of expenseEntry.items) {
          if (item?.spenderType !== "cso" || !item.spenderId) continue;
          const metric = ensureMetric(item.spenderId, {});
          if (!metric) continue;
          metric.expenses += Number(item.amount || 0);
        }
      }
    }

    const metricsArray = Array.from(metricsMap.values())
      .map((metric) => {
        const loanBalance = metric.cumulativeAmountToBePaid - metric.cumulativePayments;
        const profit = metric.interest + metric.adminFee - metric.expenses;
        const disbursementProgress = metric.disbursementTarget > 0
          ? metric.totalDisbursed / metric.disbursementTarget
          : 0;

        return {
          csoId: metric.csoId,
          csoName: metric.csoName,
          loansThisMonth: metric.loansThisMonth,
          totalDisbursed: Number(metric.totalDisbursed.toFixed(2)),
          amountToBePaid: Number(metric.amountToBePaid.toFixed(2)),
          amountPaid: Number(metric.amountPaid.toFixed(2)),
          adminFee: Number(metric.adminFee.toFixed(2)),
          interest: Number(metric.interest.toFixed(2)),
          expenses: Number(metric.expenses.toFixed(2)),
          loanBalance: Number(loanBalance.toFixed(2)),
          profit: Number(profit.toFixed(2)),
          loanTarget: metric.loanTarget,
          disbursementTarget: Number(metric.disbursementTarget || 0),
          targetMet: metric.loanTarget > 0 ? metric.loansThisMonth >= metric.loanTarget : false,
          disbursementProgress: Number(disbursementProgress.toFixed(4)),
        };
      })
      .filter((metric) => {
        if (!rawSearch) {
          return true;
        }

        const [searchId] = rawSearch.split("::");
        if (!searchId) {
          return true;
        }

        return metric.csoId === searchId;
      })
      .sort((a, b) => a.csoName.localeCompare(b.csoName));

    const summary = metricsArray.reduce(
      (acc, metric) => {
        acc.totalCsos += 1;
        acc.totalLoans += metric.loansThisMonth;
        acc.totalDisbursed += metric.totalDisbursed;
        acc.totalAmountToBePaid += metric.amountToBePaid;
        acc.totalAmountPaid += metric.amountPaid;
        acc.totalAdminFee += metric.adminFee;
        acc.totalExpenses += metric.expenses;
        acc.totalProfit += metric.profit;
        acc.targetsMet += metric.targetMet ? 1 : 0;
        return acc;
      },
      {
        totalCsos: 0,
        totalLoans: 0,
        totalDisbursed: 0,
        totalAmountToBePaid: 0,
        totalAmountPaid: 0,
        totalAdminFee: 0,
        totalExpenses: 0,
        totalProfit: 0,
        targetsMet: 0,
      }
    );

    const availableMonths = monthTimeline.map((entry) => {
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
    }).filter(Boolean);

    return res.json({
      branch: {
        id: branch._id,
        name: branch.name,
      },
      month: {
        year: safeYear,
        month: safeMonth,
      },
      data: metricsArray,
      summary: {
        totalCsos: summary.totalCsos,
        totalLoans: summary.totalLoans,
        totalDisbursed: Number(summary.totalDisbursed.toFixed(2)),
        totalAmountToBePaid: Number(summary.totalAmountToBePaid.toFixed(2)),
        totalAmountPaid: Number(summary.totalAmountPaid.toFixed(2)),
        totalAdminFee: Number(summary.totalAdminFee.toFixed(2)),
        totalExpenses: Number(summary.totalExpenses.toFixed(2)),
        totalProfit: Number(summary.totalProfit.toFixed(2)),
        targetsMet: summary.targetsMet,
      },
      filter: {
        search: rawSearch,
      },
      availableMonths,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to compute branch CSO metrics" });
  }
});

router.get("/api/branches/:id/customer-metrics", async (req, res) => {
  try {
    const { id } = req.params;
    const rawSearch = typeof req.query.search === "string" ? req.query.search.trim() : "";
    const rawCsoId = typeof req.query.csoId === "string" ? req.query.csoId.trim() : "";
    const rawPage = Number.parseInt(req.query.page, 10);
    const rawLimit = Number.parseInt(req.query.limit, 10);

    if (!mongoose.Types.ObjectId.isValid(id)) {
      return res.status(400).json({ message: "Invalid branch identifier" });
    }

    const branch = await Branch.findById(id).select("name").lean();

    if (!branch) {
      return res.status(404).json({ message: "Branch not found" });
    }

    const branchIdString = branch._id.toString();
    const branchName = typeof branch.name === "string" ? branch.name.trim() : null;

    const branchFilters = [{ branchId: branchIdString }];
    if (branchIdString !== id) {
      branchFilters.push({ branchId: id });
    }
    if (branchName) {
      branchFilters.push({ branch: branchName });
    }

    const eligibleStatuses = ["approved", "active loan", "fully paid"];

    const hasValidCsoFilter = rawCsoId && mongoose.Types.ObjectId.isValid(rawCsoId);
    const csoObjectId = hasValidCsoFilter ? new mongoose.Types.ObjectId(rawCsoId) : null;

    const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, 100) : 15;
    const requestedPage = Number.isFinite(rawPage) && rawPage > 0 ? rawPage : 1;

    const [loans, csos] = await Promise.all([
      Loan.find({
        status: { $in: eligibleStatuses },
        disbursedAt: { $ne: null },
        $or: branchFilters,
        ...(csoObjectId ? { csoId: csoObjectId } : {}),
      })
        .select(
          "customerDetails.firstName customerDetails.lastName customerDetails.bvn loanDetails.amountToBePaid loanDetails.amountPaidSoFar loanDetails.amountDisbursed status disbursedAt repaymentSchedule csoId"
        )
        .lean(),
      CSO.find({
        isActive: true,
        $or: [
          { branchId: branchIdString },
          { branchId: id },
          branchName ? { branch: branchName } : null,
        ].filter(Boolean),
      })
        .select("firstName lastName")
        .lean(),
    ]);

    const now = new Date();

    const computeEndDate = (schedule = []) => {
      if (!Array.isArray(schedule) || schedule.length === 0) {
        return null;
      }

      const timestamps = schedule
        .map((entry) => {
          if (!entry?.date) return Number.NEGATIVE_INFINITY;
          const parsed = new Date(entry.date);
          return Number.isNaN(parsed.getTime()) ? Number.NEGATIVE_INFINITY : parsed.getTime();
        })
        .filter((time) => Number.isFinite(time));

      if (timestamps.length === 0) {
        return null;
      }

      return new Date(Math.max(...timestamps));
    };

    const computeDefaults = (schedule = []) => {
      if (!Array.isArray(schedule) || schedule.length === 0) {
        return 0;
      }

      return schedule.reduce((total, entry) => {
        if (!entry?.date || entry.status !== "pending") {
          return total;
        }

        const parsed = new Date(entry.date);
        if (Number.isNaN(parsed.getTime())) {
          return total;
        }

        return parsed <= now ? total + 1 : total;
      }, 0);
    };

    const toLoanSnapshot = (loan) => {
      const disbursedAt = loan.disbursedAt ? new Date(loan.disbursedAt) : null;
      const amountToBePaid = Number(loan.loanDetails?.amountToBePaid || 0);
      const amountPaid = Number(loan.loanDetails?.amountPaidSoFar || 0);
      const endDate = computeEndDate(loan.repaymentSchedule || []);

      return {
        status: loan.status,
        disbursedAt,
        endDate,
        amountToBePaid,
        amountPaid,
        loanBalance: Number((amountToBePaid - amountPaid).toFixed(2)),
      };
    };

    const customersMap = new Map();

    for (const loan of loans) {
      const bvn = typeof loan.customerDetails?.bvn === "string" ? loan.customerDetails.bvn.trim() : "";
      if (!bvn) {
        continue;
      }

      if (!customersMap.has(bvn)) {
        const firstName = loan.customerDetails?.firstName || "";
        const lastName = loan.customerDetails?.lastName || "";
        customersMap.set(bvn, {
          bvn,
          customerName: `${firstName} ${lastName}`.trim() || "Unknown Customer",
          loansCount: 0,
          defaultsCount: 0,
          currentLoan: null,
          latestLoan: null,
        });
      }

      const customer = customersMap.get(bvn);
      customer.loansCount += 1;
      customer.defaultsCount += computeDefaults(loan.repaymentSchedule || []);

      const snapshot = toLoanSnapshot(loan);

      if (loan.status === "active loan") {
        customer.currentLoan = snapshot;
      }

      if (!customer.latestLoan) {
        customer.latestLoan = snapshot;
      } else {
        const existingDate = customer.latestLoan.disbursedAt?.getTime() || 0;
        const candidateDate = snapshot.disbursedAt?.getTime() || 0;
        if (candidateDate > existingDate) {
          customer.latestLoan = snapshot;
        }
      }
    }

    const normalizeStatus = (customer) => {
      if (customer.currentLoan) {
        return "Active loan";
      }

      if (!customer.latestLoan) {
        return "No open loan";
      }

      if (customer.latestLoan.status === "fully paid") {
        return "Fully paid";
      }

      if (customer.latestLoan.status === "approved") {
        return "Approved";
      }

      return customer.latestLoan.status || "No open loan";
    };

    const performanceForDefaults = (defaultsCount) => {
      if (defaultsCount <= 0) {
        return { score: 100, label: "Excellent" };
      }

      if (defaultsCount <= 3) {
        return { score: Math.max(70, 100 - defaultsCount * 10), label: "Fair" };
      }

      const score = Math.max(40, 100 - defaultsCount * 12);
      return { score, label: "Poor" };
    };

    const customersArray = Array.from(customersMap.values())
      .map((customer) => {
        const activeLoan = customer.currentLoan || customer.latestLoan || null;

        const performance = performanceForDefaults(customer.defaultsCount);

        const startDate = activeLoan?.disbursedAt ? activeLoan.disbursedAt.toISOString() : null;
        const endDate = activeLoan?.endDate ? activeLoan.endDate.toISOString() : null;

        return {
          bvn: customer.bvn,
          customerName: customer.customerName,
          loansCount: customer.loansCount,
          defaultsCount: customer.defaultsCount,
          amountToBePaid: activeLoan ? Number(activeLoan.amountToBePaid.toFixed(2)) : 0,
          amountPaid: activeLoan ? Number(activeLoan.amountPaid.toFixed(2)) : 0,
          loanBalance: activeLoan ? Number(activeLoan.loanBalance.toFixed(2)) : 0,
          startDate,
          endDate,
          status: normalizeStatus(customer),
          performanceScore: Number(performance.score.toFixed(1)),
          performanceLabel: performance.label,
        };
      })
      .filter((customer) => {
        if (!rawSearch) {
          return true;
        }

        const needle = rawSearch.toLowerCase();
        return (
          customer.customerName.toLowerCase().includes(needle) ||
          customer.bvn.toLowerCase().includes(needle)
        );
      })
      .sort((a, b) => a.customerName.localeCompare(b.customerName));

    const total = customersArray.length;
    const totalPages = total === 0 ? 0 : Math.ceil(total / limit);
    const safePage = totalPages === 0 ? 1 : Math.min(requestedPage, totalPages);
    const startIndex = total === 0 ? 0 : (safePage - 1) * limit;
    const paginatedData = total === 0 ? [] : customersArray.slice(startIndex, startIndex + limit);

    const pagination = {
      page: safePage,
      limit,
      total,
      totalPages,
    };

    const csoOptions = csos
      .map((cso) => ({
        id: cso._id?.toString() || "",
        name: `${cso.firstName || ""} ${cso.lastName || ""}`.trim() || "Unnamed CSO",
      }))
      .filter((option) => option.id)
      .sort((a, b) => a.name.localeCompare(b.name));

    const summary = customersArray.reduce(
      (acc, customer) => {
        acc.totalCustomers += 1;
        acc.totalLoans += customer.loansCount;
        acc.totalDefaults += customer.defaultsCount;
        acc.activeCustomers += customer.status === "Active loan" ? 1 : 0;
        return acc;
      },
      {
        totalCustomers: 0,
        totalLoans: 0,
        totalDefaults: 0,
        activeCustomers: 0,
      }
    );

    return res.json({
      branch: {
        id: branch._id,
        name: branch.name,
      },
      filter: {
        search: rawSearch,
        csoId: csoObjectId ? csoObjectId.toString() : "",
      },
      data: paginatedData,
      summary,
      pagination,
      csos: csoOptions,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to compute branch customer metrics" });
  }
});

module.exports = router;
