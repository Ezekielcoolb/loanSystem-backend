require("dotenv").config();
const mongoose = require("mongoose");
const Loan = require("./models/loan");

async function inspect() {
  try {
    if (!process.env.MONGODB_URI) {
      console.error("MONGODB_URI is missing from .env");
      process.exit(1);
    }
    await mongoose.connect(process.env.MONGODB_URI);
    console.log("Connected to DB");

    const loans = await Loan.find({}, "customerDetails loanId disbursedAt")
      .sort({ createdAt: -1 })
      .limit(5)
      .lean();

    console.log("--- SAMPLED DATA START ---");
    console.log(JSON.stringify(loans, null, 2));
    console.log("--- SAMPLED DATA END ---");
  } catch (err) {
    console.error(err);
  } finally {
    await mongoose.disconnect();
  }
}

inspect();
