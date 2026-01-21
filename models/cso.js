const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");

const csoSchema = new mongoose.Schema(
  {
    firstName: { type: String, required: true },
    lastName: { type: String, required: true },
    email: { type: String, required: true, unique: true },
    phone: { type: String, required: true },
    branch: { type: String, required: true },
    branchId: { type: String, required: true },
    address: { type: String, required: true },
    workId: { type: String, required: true },
    password: { type: String, select: false },
    guaratorName: { type: String, required: true },
    guaratorAddress: { type: String, required: true },
    guaratorPhone: { type: String, required: true },
    guaratorEmail: { type: String },
    dateOfBirth: { type: Date },
    city: { type: String },
    state: { type: String },
    zipCode: { type: String },
    country: { type: String },
    profileImg: { type: String },

    signature: { type: String },

    walletOne: {
      performanceBonus: { type: Number, default: 0 },
    },

    walletTwo: {
      amount: { type: Number, default: 0 },
    },
    isActive: { type: Boolean, default: true },
    remittance: [
      {
        amountCollected: { type: String, default: "0" },
        amount: { type: String, default: "0" }, // Legacy field support
        amountPaid: { type: String, default: "0" },
        image: { type: String },
        date: { type: Date },
        amountRemitted: { type: Number, default: 0 },
        amountOnTeller: { type: Number, default: 0 },
        issueResolution: { type: String, default: "" },
        remark: { type: String },
        resolvedIssue: { type: String, default: "" },
        partialSubmissions: [
          {
            amount: { type: Number },
            image: { type: String },
            submittedAt: { type: Date, default: Date.now },
          },
        ],
        createdAt: { type: Date, default: Date.now },
        updatedAt: { type: Date, default: Date.now },
      },
    ],

    overdueRecords: [
      {
        month: { type: Number }, // 1-12
        year: { type: Number },
        value: { type: Number, default: 0 }, // total overdue for the month
        updatedAt: { type: Date, default: Date.now },
      },
    ],

    recoveryRecord: [
      {
        month: { type: Number }, // 1-12
        year: { type: Number },
        value: { type: Number, default: 0 }, // total recovery for the month
        updatedAt: { type: Date, default: Date.now },
      },
    ],

    overShootLoans: [
      {
        month: { type: Number }, // 1-12
        year: { type: Number },
        value: { type: Number, default: 0 }, // total overshoot for the month
        countNow: { type: Number, default: 0 },
        shootCount: { type: Number, default: 0 },
        updatedAt: { type: Date, default: Date.now },
      },
    ],
    overshootPaid: [
      {
        amount: { type: Number, default: 0 }, // total overshoot for the month
        paidAt: { type: Date, default: Date.now },
      },
    ],

    // New fields for targets
    defaultingTarget: { type: Number, default: 0 }, // Default target for all CSOs

    loanTarget: { type: Number, default: 0 }, // Individual loan target for the CSO
    disbursementTarget: { type: Number, default: 0 },
  },
  { timestamps: true },
);

csoSchema.pre("save", async function hashPassword(next) {
  if (!this.isModified("password") || !this.password) {
    return next();
  }

  try {
    const salt = await bcrypt.genSalt(10);
    this.password = await bcrypt.hash(this.password, salt);
    return next();
  } catch (error) {
    return next(error);
  }
});

csoSchema.pre("findOneAndUpdate", async function hashUpdatedPassword(next) {
  const update = this.getUpdate();

  if (!update) {
    return next();
  }

  const password = update.password || update.$set?.password;

  if (!password) {
    return next();
  }

  try {
    const salt = await bcrypt.genSalt(10);
    const hashed = await bcrypt.hash(password, salt);

    if (update.password) {
      update.password = hashed;
    }

    if (update.$set?.password) {
      update.$set.password = hashed;
    }

    return next();
  } catch (error) {
    return next(error);
  }
});

csoSchema.methods.comparePassword = async function comparePassword(candidate) {
  if (!this.password) {
    return false;
  }

  try {
    const match = await bcrypt.compare(candidate, this.password);
    if (match) {
      return true;
    }
  } catch (error) {
    // fall through to plain-text comparison
  }

  return this.password === candidate;
};

csoSchema.methods.toJSON = function toJSON() {
  const obj = this.toObject();
  delete obj.password;
  return obj;
};

const CSO = mongoose.model("CSO", csoSchema);

module.exports = CSO;
