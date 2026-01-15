const mongoose = require("mongoose");

const interestSchema = new mongoose.Schema({
  amount: { type: Number, required: true },
  description: { type: String, required: true },
  createdAt: { type: Date, default: Date.now },
});

module.exports = mongoose.models.Interest || mongoose.model("Interest", interestSchema);
