const mongoose = require("mongoose");
require("dotenv").config();
const Loan = require("./models/loan");
const CSO = require("./models/cso");

async function checkDuplicates() {
  try {
    await mongoose.connect(process.env.MONGO_URI);
    console.log("Connected to MongoDB");

    const now = new Date();
    const monthStart = new Date(2026, 0, 1);
    const monthEnd = new Date(2026, 1, 1);

    const loans = await Loan.find({
      disbursedAt: { $gte: monthStart, $lt: monthEnd },
      status: { $in: ["approved", "active loan", "fully paid"] },
    });

    console.log(`Found ${loans.length} loans for Jan 2026`);

    const nameToDetails = {};

    // Check Loans
    loans.forEach((l) => {
      if (!l.csoName || !l.csoId) return;
      const name = l.csoName.trim();
      const id = l.csoId.toString();

      if (!nameToDetails[name]) {
        nameToDetails[name] = { ids: new Set(), sources: [] };
      }
      nameToDetails[name].ids.add(id);
      nameToDetails[name].sources.push("Loan");
    });

    // Check CSOs
    const csos = await CSO.find({});
    csos.forEach((c) => {
      const name = `${c.firstName || ""} ${c.lastName || ""}`.trim();
      const id = c._id.toString();
      if (!nameToDetails[name]) {
        nameToDetails[name] = { ids: new Set(), sources: [] };
      }
      nameToDetails[name].ids.add(id);
      nameToDetails[name].sources.push("CSO Record");
    });

    console.log("\nCSOs with multiple IDs:");
    let found = false;
    Object.keys(nameToDetails).forEach((name) => {
      if (nameToDetails[name].ids.size > 1) {
        console.log(
          `- ${name}: [${Array.from(nameToDetails[name].ids).join(", ")}]`
        );
        found = true;
      }
    });

    if (!found) {
      console.log("No CSOs with multiple IDs found.");
    }

    process.exit();
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

checkDuplicates();
