const mongoose = require("mongoose");
const dotenv = require("dotenv");
const path = require("path");
const CSO = require("./models/cso");

dotenv.config();

const dbUri = process.env.MONGO_URI;
if (!dbUri) {
  console.error("MONGO_URI not found in .env");
  process.exit(1);
}

mongoose
  .connect(dbUri)
  .then(async () => {
    const id = "67f839bbd1dc5ecb40bf41b9";
    const cso = await CSO.findById(id);
    if (cso) {
      console.log("CSO found:", cso.email);
    } else {
      console.log("CSO not found");
    }
    process.exit(0);
  })
  .catch((err) => {
    console.error("Connection error:", err);
    process.exit(1);
  });
