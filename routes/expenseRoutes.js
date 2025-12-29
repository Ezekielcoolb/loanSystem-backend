const express = require("express");
const Report = require("../models/Report");
const AdminMember = require("../models/adminPanel");
const CSO = require("../models/cso");

const router = express.Router();

function toLagosDate(value) {
  const date = value ? new Date(value) : new Date();

  if (Number.isNaN(date.getTime())) {
    return null;
  }

  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Africa/Lagos",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });

  const dateKey = formatter.format(date);
  const normalizedDate = new Date(`${dateKey}T00:00:00.000+01:00`);
  return { dateKey, normalizedDate };
}

function isWeekend(date) {
  if (!date) {
    return true;
  }

  const weekdayFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "Africa/Lagos",
    weekday: "short",
  });

  const weekday = weekdayFormatter.format(date);
  return weekday === "Sat" || weekday === "Sun";
}

async function ensureReportDocument() {
  const existing = await Report.findOne();
  if (existing) {
    return existing;
  }
  return Report.create({});
}

function sumExpenses(items = []) {
  return items.reduce((total, item) => total + Number(item.amount || 0), 0);
}

function sortItemsBySubmittedAt(items = []) {
  return [...items].sort(
    (a, b) =>
      new Date(b?.submittedAt || b?.createdAt || 0) -
      new Date(a?.submittedAt || a?.createdAt || 0)
  );
}

function sortCashEntries(entries = []) {
  return [...entries].sort((a, b) => b.date.localeCompare(a.date));
}

router.get("/api/expenses", async (req, res) => {
  try {
    const queryDateInput = req.query.date
      ? toLagosDate(req.query.date)
      : null;
    const queryDate = queryDateInput?.dateKey || null;
    const report = await Report.findOne();

    if (!report) {
      if (queryDate) {
        return res.json({ date: queryDate, items: [], totalAmount: 0 });
      }

      return res.json({ entries: [], totalAmount: 0 });
    }

    const entries = [...(report.expenses || [])].sort((a, b) =>
      b.date.localeCompare(a.date)
    );

    if (queryDate) {
      const entry = entries.find((item) => item.date === queryDate);
      const sortedItems = entry ? sortItemsBySubmittedAt(entry.items) : [];
      return res.json({
        date: queryDate,
        items: sortedItems.map((item) =>
          item.toObject ? item.toObject() : item
        ),
        totalAmount: sumExpenses(sortedItems),
      });
    }

    const mappedEntries = entries.map((entry) => {
      const entryItems = sortItemsBySubmittedAt(entry.items || []);
      return {
        date: entry.date,
        totalAmount: sumExpenses(entryItems),
        count: entryItems.length,
        items: entryItems.map((item) =>
          item.toObject ? item.toObject() : item
        ),
      };
    });

    const totalAmount = mappedEntries.reduce(
      (total, entry) => total + entry.totalAmount,
      0
    );

    return res.json({ entries: mappedEntries, totalAmount });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch expenses" });
  }
});

router.post("/api/expenses", async (req, res) => {
  try {
    const {
      amount,
      purpose,
      date,
      receiptImg,
      spenderType,
      spenderId,
    } = req.body || {};

    const normalizedAmount = Number(amount);

    if (!Number.isFinite(normalizedAmount) || normalizedAmount <= 0) {
      return res
        .status(400)
        .json({ message: "Amount must be a positive number" });
    }

    if (!purpose || !purpose.trim()) {
      return res.status(400).json({ message: "Purpose is required" });
    }

    if (!receiptImg || !receiptImg.trim()) {
      return res.status(400).json({
        message: "Receipt image is required. Upload via the upload manager.",
      });
    }

    const lagosDate = toLagosDate(date || new Date());
    if (!lagosDate) {
      return res.status(400).json({ message: "Invalid date supplied" });
    }

    if (isWeekend(lagosDate.normalizedDate)) {
      return res
        .status(400)
        .json({ message: "Expenses cannot be recorded on weekends" });
    }
    const dateKey = lagosDate.dateKey;

    let resolvedSpender = {
      spenderType: "super_admin",
      spenderId: null,
      spenderName: "Super Admin",
    };

    if (spenderType === "cso" && spenderId) {
      const cso = await CSO.findById(spenderId).select("firstName lastName");
      if (!cso) {
        return res.status(404).json({ message: "CSO not found" });
      }
      resolvedSpender = {
        spenderType: "cso",
        spenderId,
        spenderName: [cso.firstName, cso.lastName].filter(Boolean).join(" "),
      };
    } else if (spenderType === "admin" && spenderId) {
      const admin = await AdminMember.findById(spenderId).select(
        "firstName lastName"
      );
      if (!admin) {
        return res.status(404).json({ message: "Admin member not found" });
      }
      resolvedSpender = {
        spenderType: "admin",
        spenderId,
        spenderName: [admin.firstName, admin.lastName].filter(Boolean).join(
          " "
        ),
      };
    }

    const report = await ensureReportDocument();
    let dayEntry = report.expenses.find((entry) => entry.date === dateKey);

    if (!dayEntry) {
      dayEntry = { date: dateKey, items: [] };
      report.expenses.push(dayEntry);
    }

    dayEntry.items.unshift({
      amount: normalizedAmount,
      purpose: purpose.trim(),
      receiptImg: receiptImg.trim(),
      submittedAt: new Date(),
      ...resolvedSpender,
    });

    report.markModified("expenses");
    await report.save();

    const savedEntry = report.expenses.find((entry) => entry.date === dateKey);
    const createdItem =
      savedEntry && savedEntry.items.length > 0
        ? savedEntry.items[0].toObject()
        : null;

    return res.status(201).json({
      message: "Expense recorded successfully",
      date: dateKey,
      item: createdItem,
      totalAmount: sumExpenses(savedEntry?.items || []),
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to record expense" });
  }
});

router.patch("/api/expenses/:expenseId/move", async (req, res) => {
  try {
    const { expenseId } = req.params;
    const { targetDate } = req.body || {};

    if (!expenseId) {
      return res.status(400).json({ message: "Expense ID is required" });
    }

    const target = toLagosDate(targetDate);
    if (!target) {
      return res.status(400).json({ message: "Invalid target date" });
    }

    if (isWeekend(target.normalizedDate)) {
      return res
        .status(400)
        .json({ message: "Cannot move expenses to a weekend" });
    }
    const targetDateKey = target.dateKey;

    const report = await Report.findOne();
    if (!report) {
      return res.status(404).json({ message: "No expenses found" });
    }

    let sourceEntryIndex = -1;
    let expenseIndex = -1;

    report.expenses.forEach((entry, entryIndex) => {
      const index = entry.items.findIndex(
        (item) => item._id.toString() === expenseId
      );
      if (index !== -1) {
        sourceEntryIndex = entryIndex;
        expenseIndex = index;
      }
    });

    if (sourceEntryIndex === -1 || expenseIndex === -1) {
      return res.status(404).json({ message: "Expense item not found" });
    }

    const sourceEntry = report.expenses[sourceEntryIndex];
    const [expenseItem] = sourceEntry.items.splice(expenseIndex, 1);

    if (!expenseItem) {
      return res.status(404).json({ message: "Expense item not found" });
    }

    const sourceDateKey = sourceEntry.date;

    let targetEntry = report.expenses.find(
      (entry) => entry.date === targetDateKey
    );

    if (!targetEntry) {
      targetEntry = { date: targetDateKey, items: [] };
      report.expenses.push(targetEntry);
    }

    expenseItem.movedAt = new Date();
    targetEntry.items.unshift(expenseItem);

    if (sourceEntry.items.length === 0) {
      report.expenses.splice(sourceEntryIndex, 1);
    }

    report.markModified("expenses");
    await report.save();

    return res.json({
      message: "Expense moved successfully",
      sourceDate: sourceDateKey,
      targetDate: targetDateKey,
      item: expenseItem.toObject ? expenseItem.toObject() : expenseItem,
      targetTotal: sumExpenses(targetEntry.items),
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to move expense" });
  }
});

router.get("/api/cash-at-hand", async (req, res) => {
  try {
    const report = await Report.findOne();
    if (!report) {
      return res.json(
        req.query.date
          ? { date: req.query.date, amount: 0, updatedAt: null }
          : { entries: [] }
      );
    }

    if (req.query.date) {
      const target = toLagosDate(req.query.date);
      if (!target) {
        return res.status(400).json({ message: "Invalid date supplied" });
      }
      const entry =
        report.cashAtHand?.find((item) => item.date === target.dateKey) || null;
      return res.json({
        date: target.dateKey,
        amount: entry?.amount || 0,
        updatedAt: entry?.updatedAt || null,
      });
    }

    return res.json({
      entries: sortCashEntries(report.cashAtHand || []).map((entry) =>
        entry.toObject ? entry.toObject() : entry
      ),
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to fetch cash at hand" });
  }
});

router.post("/api/cash-at-hand", async (req, res) => {
  try {
    const { amount, date } = req.body || {};
    const normalizedAmount = Number(amount);

    if (!Number.isFinite(normalizedAmount) || normalizedAmount < 0) {
      return res.status(400).json({
        message: "Amount must be a non-negative number",
      });
    }

    const lagosDate = toLagosDate(date || new Date());
    if (!lagosDate) {
      return res.status(400).json({ message: "Invalid date supplied" });
    }

    const report = await ensureReportDocument();
    const existingEntry = report.cashAtHand.find(
      (entry) => entry.date === lagosDate.dateKey
    );

    if (existingEntry) {
      existingEntry.amount = normalizedAmount;
      existingEntry.updatedAt = new Date();
    } else {
      report.cashAtHand.push({
        date: lagosDate.dateKey,
        amount: normalizedAmount,
        updatedAt: new Date(),
      });
    }

    report.markModified("cashAtHand");
    await report.save();

    return res.status(201).json({
      message: "Cash at hand updated",
      date: lagosDate.dateKey,
      amount: normalizedAmount,
    });
  } catch (error) {
    return res
      .status(500)
      .json({ message: error.message || "Unable to update cash at hand" });
  }
});

module.exports = router;
