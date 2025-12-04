const cron = require("node-cron");
const schedule = require("node-schedule");
const sendSubmissionIdToLambda = require("./sendSubmissionIdToLambda");
const sendForms = require("./sendForms");
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
async function scheduledTasksSendSubmissionIDToLambda() {
  try {
    await Promise.all([
      sendSubmissionIdToLambda(),
      // add more task functions here
    ]);

    console.log(
      "scheduledTasksSendSubmissionIDToLambda executed successfully."
    );
  } catch (error) {
    console.error(
      "Error executing scheduledTasksSendSubmissionIDToLambda:",
      error.message
    );
  }
}

// Cron: every minute, timezone Europe/London
cron.schedule("* * * * *", scheduledTasksSendSubmissionIDToLambda, {
  scheduled: true,
  timezone: "Europe/London",
});
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
async function scheduledTasksSendForms() {
  try {
    await Promise.all([
      sendForms(),
      // add more task functions here
    ]);

    console.log("scheduledTasksSendForms executed successfully.");
  } catch (error) {
    console.error("Error executing scheduledTasksSendForms:", error.message);
  }
}

// Cron-style: Every 5 seconds
schedule.scheduleJob("*/5 * * * * *", scheduledTasksSendForms);
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

// Run at 0s
// cron.schedule("*/1 * * * *", scheduledTasks, {
//   scheduled: true,
//   timezone: "Europe/London",
// });

// Run at 30s (workaround)
// setTimeout(() => {
//   cron.schedule("*/1 * * * *", scheduledTasks, {
//     scheduled: true,
//     timezone: "Europe/London",
//   });
// }, 30 * 1000);
