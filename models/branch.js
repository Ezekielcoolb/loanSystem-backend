const mongoose = require('mongoose');

const branchSchema = new mongoose.Schema(
    {
        name: {
            type: String,
            required: true,
            trim: true,
        },
        supervisorName: {
            type: String,
            required: true,
            trim: true,
        },
        supervisorEmail: {
            type: String,
            required: true,
            unique: true,
            match: [/^\S+@\S+\.\S+$/, 'Please use a valid email address'],
        },
        supervisorPhone: {
            type: String,
            required: true,
            match: [/^\d{10,15}$/, 'Please use a valid phone number'],
        },
        address: {
            type: String,
            required: true,
            trim: true,
        },
        city: {
            type: String,
            required: true,
            trim: true,
        },
        state: {
            type: String,
            required: true,
            trim: true,
        },
        
        country: {
            type: String,
            required: true,
            trim: true,
        },
        loanTarget: { type: Number, default: 0 },
        disbursementTarget: { type: Number, default: 0 },
    },
    {
        timestamps: true, // Adds createdAt and updatedAt timestamps
    }
);

module.exports = mongoose.model('Branch', branchSchema);
