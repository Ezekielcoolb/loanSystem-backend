const mongoose = require("mongoose");

const holidaySchema = new mongoose.Schema(
  {
    holiday: { type: Date, required: true },
    reason: { type: String, trim: true },
    isRecurring: { type: Boolean, default: false },
    recurringKey: { type: String, default: null }, // MM-DD format for recurring holidays
  },
  { timestamps: true }
);

holidaySchema.pre("save", function setRecurringKey(next) {
  if (this.isRecurring && this.holiday instanceof Date && !Number.isNaN(this.holiday.valueOf())) {
    const month = String(this.holiday.getUTCMonth() + 1).padStart(2, "0");
    const day = String(this.holiday.getUTCDate()).padStart(2, "0");
    this.recurringKey = `${month}-${day}`;
  } else {
    this.recurringKey = null;
  }
  next();
});

holidaySchema.index({ holiday: 1 });
holidaySchema.index({ recurringKey: 1 }, { sparse: true });

module.exports = mongoose.model("Holiday", holidaySchema);
