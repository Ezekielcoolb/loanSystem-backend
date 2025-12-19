const mongoose = require("mongoose");

const groupLeaderSchema = new mongoose.Schema(
  {
    groupName: { type: String, required: true },
    firstName: { type: String, required: true },
    lastName: { type: String, required: true },
    address: { type: String, required: true },
    phone: { type: String, required: true, unique: true },
    csoId: { type: String, required: true },
    csoName: { type: String, required: true },
    status: {
      type: String,
      enum: ["waiting for approval", "approved", "rejected"],
      default: "waiting for approval",
    },
  },
  { timestamps: true }
);

groupLeaderSchema.index({ csoId: 1, createdAt: -1 });
groupLeaderSchema.index({ createdAt: -1 });

const GroupLeader = mongoose.model("GroupLeader", groupLeaderSchema);

module.exports = GroupLeader;
