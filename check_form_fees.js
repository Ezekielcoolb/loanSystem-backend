const mongoose = require("mongoose");
require("dotenv").config();
const Loan = require("./models/loan");

async function checkFormFees() {
  try {
    await mongoose.connect(process.env.MONGO_URI);
    console.log("Connected to MongoDB");

    const totalLoans = await Loan.countDocuments({});
    const withFee = await Loan.countDocuments({
      "loanDetails.loanAppForm": { $exists: true, $gt: 0 },
    });
    const zeroFee = await Loan.countDocuments({ "loanDetails.loanAppForm": 0 });
    const missingFee = await Loan.countDocuments({
      "loanDetails.loanAppForm": { $exists: false },
    });

    console.log(`Total Loans: ${totalLoans}`);
    console.log(`Loans with fee > 0: ${withFee}`);
    console.log(`Loans with fee = 0: ${zeroFee}`);
    console.log(`Loans missing fee field: ${missingFee}`);

    if (totalLoans > 0) {
      const sample = await Loan.findOne({
        "loanDetails.loanAppForm": { $exists: true },
      }).lean();
      if (sample) {
        console.log(
          "Sample loanDetails:",
          JSON.stringify(sample.loanDetails, null, 2)
        );
      }
    }

    process.exit();
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

checkFormFees();
