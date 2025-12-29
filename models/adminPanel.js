const mongoose = require("mongoose");

const VALID_ROLES = [
  "Manager",
  "Disbursement Officer",
  "Support/Reconciliation Officer",
  "Agency Manager",
];

const adminMemberSchema = new mongoose.Schema(
  {
    firstName: { type: String, required: true, trim: true },
    lastName: { type: String, required: true, trim: true },
    email: {
      type: String,
      required: true,
      trim: true,
      lowercase: true,
      unique: true,
    },
    phone: { type: String, required: true, trim: true },
    password: { type: String, required: true },
    assignedRole: {
      type: String,
      required: true,
      enum: VALID_ROLES,
    },
    gender: { type: String, required: true, trim: true },
    isSuspended: { type: Boolean, default: false },
  },
  { timestamps: true }
);

const AdminMember = mongoose.model("AdminPanel", adminMemberSchema);
module.exports = AdminMember;
