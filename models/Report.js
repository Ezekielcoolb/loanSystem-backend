const mongoose = require("mongoose");

const expenseItemSchema = new mongoose.Schema(
  {
    amount: {
      type: Number,
      required: true,
      min: 0,
    },
    purpose: {
      type: String,
      required: true,
      trim: true,
    },
    spenderId: {
      type: String,
      default: null,
    },
    spenderName: {
      type: String,
      default: "Super Admin",
      trim: true,
    },
    spenderType: {
      type: String,
      enum: ["cso", "admin", "super_admin"],
      default: "super_admin",
    },
    receiptImg: {
      type: String,
      required: true,
      trim: true,
    },
    submittedAt: {
      type: Date,
      default: Date.now,
    },
    movedAt: {
      type: Date,
    },
  },
  { _id: true }
);

const reportSchema = new mongoose.Schema({
  expenses: {
    type: [
      {
        date: {
          type: String,
          required: true,
        }, // e.g., "2025-05-21"
        items: [expenseItemSchema],
      },
    ],
    default: [],
  },
  cashAtHand: {
    type: [
      {
        date: {
          type: String,
          required: true,
        },
        amount: {
          type: Number,
          required: true,
          min: 0,
        },
        updatedAt: {
          type: Date,
          default: Date.now,
        },
      },
    ],
    default: [],
  },
});

reportSchema.index({ "expenses.date": 1 });
reportSchema.index({ "cashAtHand.date": 1 });

module.exports = mongoose.model("Report", reportSchema);
