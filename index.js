const express = require('express');
const cors = require('cors');
const qs = require("qs");
const axios = require("axios");
const dotenv = require('dotenv');
const connectDB = require('./config/config');
const path = require('path');
const { scheduleCsoDelinquencyJob } = require('./jobs/csoDelinquencyJob');
const app = express();
dotenv.config();

connectDB()
  .then(() => {
    scheduleCsoDelinquencyJob();
  })
  .catch((error) => {
    console.error('Database initialization failed:', error);
  });

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// Routes
app.use(require("./routes/branchRoutes"));
app.use(require("./routes/csoRoutes"));
app.use(require("./routes/loanRoutes"));
app.use(require("./routes/holidayRoutes"));
app.use(require("./routes/adminPanelRoutes"));
app.use(require("./routes/expenseRoutes"));
app.use(require('./routes/uploadRoutes'));
app.use(require("./routes/businessReportRoutes"));
app.use(require("./routes/interestRoutes"));

// Error handling
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).send('Something broke!');
});

// Start server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
