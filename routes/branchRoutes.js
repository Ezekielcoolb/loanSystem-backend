const express = require("express");
const Branch = require("../models/branch");

const router = express.Router();

// Create a new branch
router.post("/api/branches", async (req, res) => {
  try {
    const branch = await Branch.create(req.body);
    return res.status(201).json(branch);
  } catch (error) {
    if (error.code === 11000) {
      return res.status(409).json({ message: "Supervisor email already exists" });
    }
    return res.status(400).json({ message: error.message || "Unable to create branch" });
  }
});

// Retrieve all branches
router.get("/api/branches", async (_req, res) => {
  try {
    const branches = await Branch.find().sort({ createdAt: -1 });
    return res.json(branches);
  } catch (error) {
    return res.status(500).json({ message: "Unable to fetch branches" });
  }
});

// Delete a branch
router.delete("/api/branches/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const deleted = await Branch.findByIdAndDelete(id);

    if (!deleted) {
      return res.status(404).json({ message: "Branch not found" });
    }

    return res.json({ message: "Branch deleted" });
  } catch (error) {
    return res.status(500).json({ message: "Unable to delete branch" });
  }
});

// Update branch targets
router.patch("/api/branches/:id/targets", async (req, res) => {
  try {
    const { id } = req.params;
    const { loanTarget, disbursementTarget } = req.body;

    const branch = await Branch.findByIdAndUpdate(
      id,
      {
        $set: {
          loanTarget: typeof loanTarget === "number" ? loanTarget : undefined,
          disbursementTarget:
            typeof disbursementTarget === "number" ? disbursementTarget : undefined,
        },
      },
      { new: true, runValidators: true }
    );

    if (!branch) {
      return res.status(404).json({ message: "Branch not found" });
    }

    return res.json(branch);
  } catch (error) {
    return res.status(400).json({ message: error.message || "Unable to update branch targets" });
  }
});

module.exports = router;
