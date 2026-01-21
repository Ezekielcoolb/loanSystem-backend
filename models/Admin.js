const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");

const adminSchema = new mongoose.Schema(
  {
    email: {
      type: String,
      required: true,
      unique: true,
      lowercase: true,
      trim: true,
    },
    password: {
      type: String,
      required: true,
      select: false,
    },
  },
  { timestamps: true }
);

adminSchema.pre("save", async function hashPassword(next) {
  if (!this.isModified("password")) {
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

adminSchema.methods.comparePassword = async function comparePassword(candidate) {
  if (!this.password) {
    return false;
  }

  try {
    const isMatch = await bcrypt.compare(candidate, this.password);
    if (isMatch) {
      return true;
    }
  } catch (_error) {
    // If bcrypt comparison fails (e.g., legacy plaintext), fall back below
  }

  // Legacy fallback: stored password is plaintext
  return this.password === candidate;
};

const Admin = mongoose.model("Admin", adminSchema);
module.exports = Admin;
