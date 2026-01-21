const mongoose = require("mongoose");

function generateLoanId() {
  const timestamp = Date.now();
  const random = Math.random().toString(36).slice(-4).toUpperCase();
  return `LN-${timestamp}-${random}`;
}

const repaymentScheduleSchema = new mongoose.Schema(
  {
    date: { type: Date, required: true },
    status: {
      type: String,
      enum: ["pending", "paid", "holiday", "partial", "approved", "submitted"],
      default: "pending",
    },
    amountPaid: { type: Number, default: 0 },
    holidayReason: { type: String },
  },
  { _id: false }
);

const dailyPaymentSchema = new mongoose.Schema(
  {
    amount: { type: Number, required: true },
    date: { type: Date, required: true },
  },
  { _id: false }
);

const loanSchema = new mongoose.Schema(
  {
    csoId: { type: mongoose.Schema.Types.ObjectId, ref: "CSO", required: true },
    csoSignature: { type: String, required: true },
    branch: { type: String, required: true },
    branchId: { type: String, required: true },
    csoName: { type: String, required: true },
    loanId: { type: String, required: true, unique: true },
    customerDetails: {
      firstName: { type: String, required: true },
      lastName: { type: String, required: true },
      dateOfBirth: { type: String },
      phoneOne: { type: String, required: true },
      address: { type: String, required: true },
      nin: { type: String }, // Made optional - BVN will be primary
      bvn: { type: String, required: true },
      NextOfKin: { type: String, required: true },
      NextOfKinNumber: { type: String, required: true },
    },
    businessDetails: {
      businessName: { type: String, required: true },
      natureOfBusiness: { type: String, required: true },
      address: { type: String, required: true },
      yearsHere: { type: String },
      nameKnown: { type: String, required: true },
      estimatedValue: { type: Number },
    },
    bankDetails: {
      accountName: { type: String, required: true },
      bankName: { type: String, required: true },
      accountNo: { type: String, required: true },
    },
    loanDetails: {
      amountRequested: { type: Number, required: true },
      loanType: { type: String, enum: ["daily", "weekly"], required: true },
      amountApproved: { type: Number },
      interest: { type: Number },
      interestRate: { type: Number },
      disbursementPicture: { type: String },
      amountToBePaid: { type: Number },
      dailyAmount: { type: Number },
      dailyPayment: { type: [dailyPaymentSchema], default: [] },
      amountPaidSoFar: { type: Number, default: 0 },
      amountDisbursed: { type: Number },
      loanAppForm: { type: Number, default: 2000 },
      insurranceFee: { type: Number, default: 2000 },
      penalty: { type: Number, default: 0 },
      penaltyPaid: { type: Number, default: 0 },
    },
    guarantorDetails: {
      name: { type: String, required: true },
      address: { type: String, required: true },
      phone: { type: String, required: true },
      relationship: { type: String, required: true },
      yearsKnown: { type: Number, required: true },
      signature: { type: String, required: true },
    },
    groupDetails: {
      groupName: { type: String, required: true },
      leaderName: { type: String, required: true },
      address: { type: String },
      groupId: { type: String, required: true },
      mobileNo: { type: String },
    },
    guarantorFormPic: { type: String },
    pictures: {
      customer: { type: String, required: true },
      business: { type: String, required: true },
      disclosure: { type: String, required: true },
      signature: { type: String, required: true },
    },
    callChecks: {
      callCso: { type: Boolean, default: false },
      callCustomer: { type: Boolean, default: false },
      callGuarantor: { type: Boolean, default: false },
      callGroupLeader: { type: Boolean, default: false },
    },
    status: {
      type: String,
      enum: [
        "waiting for approval",
        "approved",

        "active loan",
        "fully paid",
        "rejected",
        "edited",
      ],
      default: "waiting for approval",
    },
    rejectionReason: { type: String },
    editedReason: { type: String },
    disbursedAt: { type: Date },
    repaymentSchedule: { type: [repaymentScheduleSchema], default: [] },
  },
  { timestamps: true }
);

loanSchema.statics.generateLoanId = generateLoanId;

loanSchema.pre("validate", function ensureLoanId(next) {
  if (!this.loanId) {
    this.loanId = this.constructor.generateLoanId();
  }

  next();
});

loanSchema.index({ disbursedAt: 1 });
loanSchema.index({ csoId: 1, createdAt: -1 });
loanSchema.index({ loanId: 1 });
loanSchema.index({ "customerDetails.bvn": 1 });

module.exports = mongoose.model("Loan", loanSchema);
