const cron = require("node-cron");
const { updateCsoDelinquencyRecords } = require("../services/csoDelinquencyService");

const DEFAULT_CRON = "20 2 * * *"; // 02:20 every day

function logSummary(summary) {
  if (!summary) {
    return;
  }

  const { month, year, updated, processed, errors, asOf } = summary;
  const prefix = `[CSO Delinquency Job]`;

  console.info(
    `${prefix} Completed for ${year}-${String(month).padStart(2, "0")} (asOf=${asOf}). Updated ${updated} of ${processed} CSOs.`
  );

  if (Array.isArray(errors) && errors.length > 0) {
    for (const error of errors) {
      console.error(`${prefix} Failed to update CSO ${error.csoId}: ${error.message}`);
    }
  }
}

async function runCsoDelinquencyJob(options = {}) {
  try {
    const summary = await updateCsoDelinquencyRecords(options);
    logSummary(summary);
    return summary;
  } catch (error) {
    console.error("[CSO Delinquency Job] Unexpected failure:", error);
    throw error;
  }
}

function scheduleCsoDelinquencyJob() {
  const cronExpression = process.env.CSO_DELINQUENCY_CRON || DEFAULT_CRON;

  cron.schedule(cronExpression, async () => {
    try {
      await runCsoDelinquencyJob();
    } catch (error) {
      // already logged inside runCsoDelinquencyJob
    }
  });

  console.info(
    `[CSO Delinquency Job] Scheduled with cron expression "${cronExpression}"`
  );
}

module.exports = {
  scheduleCsoDelinquencyJob,
  runCsoDelinquencyJob,
};
