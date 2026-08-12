import cron from "node-cron";
import axios from "axios";
import { Key } from "../models/index.js";
import { getRMHeaders } from "../utils/rentManager.js";
import sendMail from "../methods/sendMail.js";

/**
 * 1. Cron function to check Rent Manager ServiceManagerIssue status
 * for keys with status "Checked Out".
 * If RM status name includes "Completed", "Withdrawn", or "Invoiced - Need Keys",
 * update DB key status to "To Be Returned".
 */
export const updateKeyStatus = async () => {
  console.log(
    "[UpdateKeyStatus] Running key status update cron job at",
    new Date().toISOString()
  );

  try {
    const keysToCheck = await Key.find({
      status: "Checked Out",
      serviceIssue: { $exists: true, $ne: "", $nin: [null, ""] },
      isDeleted: { $ne: true },
    });

    if (!keysToCheck || keysToCheck.length === 0) {
      console.log(
        "[UpdateKeyStatus] No Checked Out keys with serviceIssue found."
      );
      return;
    }

    console.log(
      `[UpdateKeyStatus] Found ${keysToCheck.length} Checked Out keys to inspect.`
    );

    const headers = await getRMHeaders();
    const baseUrl = process.env.RM_BASE_URL;
    let updatedCount = 0;

    const targetKeywords = ["Completed", "Withdrawn", "Invoiced - Need Keys"];

    for (const key of keysToCheck) {
      const serviceIssueId = String(key.serviceIssue || "").trim();
      if (!serviceIssueId) continue;

      try {
        const res = await axios.get(
          `${baseUrl}/ServiceManagerIssues/${serviceIssueId}`,
          {
            headers,
            params: {
              embeds: "Status",
              fields: "Description,Status,StatusID,Title",
            },
            timeout: 10000,
          }
        );

        const rmStatus = res.data?.Status?.Name || res.data?.StatusName || "";
        console.log({ rmStatus }, { serviceIssueId });
        const statusNameLower = rmStatus.toLowerCase();

        const matchesKeyword = targetKeywords.some((keyword) =>
          statusNameLower.includes(keyword.toLowerCase())
        );

        if (matchesKeyword) {
          console.log(
            `[UpdateKeyStatus] Key ${key._id} (ServiceIssue: ${serviceIssueId}) RM status is "${rmStatus}". Updating key status to "To Be Returned".`
          );

          const logMsg = `${new Date().toLocaleString()}: Status updated via RM cron to "To Be Returned" (ServiceIssue: ${serviceIssueId}, RM Status: "${rmStatus}")`;

          key.status = "To Be Returned";
          key.isReturned = false;
          if (!Array.isArray(key.accessLog)) {
            key.accessLog = [];
          }
          key.accessLog.push(logMsg);

          try {
            await key.save();
          } catch (saveErr) {
            await Key.updateOne(
              { _id: key._id },
              {
                $set: {
                  status: "To Be Returned",
                  isReturned: false,
                },
                $push: { accessLog: logMsg },
              }
            );
          }
          updatedCount++;
        }
      } catch (err) {
        console.warn(
          `[UpdateKeyStatus] Failed to fetch RM issue ${serviceIssueId} for key ${key._id}:`,
          err.message
        );
      }
    }

    console.log(
      `[UpdateKeyStatus] Finished checking. Updated ${updatedCount} keys to "To Be Returned".`
    );
  } catch (error) {
    console.error("[UpdateKeyStatus] Error in key status cron job:", error);
  }
};

/**
 * 2. Weekly Cron function to send reminders to vendors/users for keys with
 * status "To Be Returned" or "Checked Out" that have a serviceIssue.
 * Prevents duplicate emails using `weeklyReminderSentAt`.
 * Runs every Monday at 8:00 AM.
 */
export const sendWeeklyKeyReminders = async () => {
  console.log(
    "[WeeklyKeyReminder] Running weekly key reminder email job at",
    new Date().toISOString()
  );

  try {
    const sixDaysAgo = new Date(Date.now() - 6 * 24 * 60 * 60 * 1000);

    const matchingKeys = await Key.find({
      status: { $in: ["To Be Returned", "Checked Out"] },
      // serviceIssue: { $exists: true, $ne: "", $nin: [null, ""] },
      isDeleted: { $ne: true },
      $or: [
        { weeklyReminderSentAt: { $exists: false } },
        { weeklyReminderSentAt: null },
        { weeklyReminderSentAt: { $lt: sixDaysAgo } },
      ],
    });

    if (!matchingKeys || matchingKeys.length === 0) {
      console.log(
        "[WeeklyKeyReminder] No outstanding keys found for weekly reminder."
      );
      return;
    }

    // Group keys by vendor / email
    const groupsByEmail = {};

    for (const key of matchingKeys) {
      const recipientEmail =
        (key?.pickerEmail || key?.email || "").trim() ||
        "turnovers@premiumpd.com";
      const vendorName = (key?.whoWillPickUp || key?.vendor || "Vendor").trim();

      if (!groupsByEmail[recipientEmail]) {
        groupsByEmail[recipientEmail] = {
          vendor: vendorName,
          email: recipientEmail,
          keys: [],
        };
      }
      groupsByEmail[recipientEmail].keys.push(key);
    }

    let sentCount = 0;
    for (const [email, group] of Object.entries(groupsByEmail)) {
      try {
        const subject = `[Action Required] Weekly Outstanding Keys Reminder - ${group.vendor}`;
        const templateName = "template-weeklyKeyReminder";
        const payload = { vendor: group.vendor, keys: group.keys };
        const ccEmail = process.env.TEST_BCC_EMAIL || "testrohit1993@gmail.com";

        await sendMail(subject, payload, email, templateName, ccEmail);
        console.log(
          `[WeeklyKeyReminder] Reminder sent to ${email} (${group.keys.length} keys)`
        );

        // Mark sent date to avoid duplicates
        const keyIds = group.keys.map((k) => k._id);
        await Key.updateMany(
          { _id: { $in: keyIds } },
          { $set: { weeklyReminderSentAt: new Date() } }
        );

        sentCount++;
      } catch (mailErr) {
        console.error(
          `[WeeklyKeyReminder] Failed to send reminder email to ${email}:`,
          mailErr.message
        );
      }
    }

    console.log(
      `[WeeklyKeyReminder] Finished sending weekly reminders. Sent ${sentCount} emails.`
    );
  } catch (error) {
    console.error(
      "[WeeklyKeyReminder] Error in weekly key reminder job:",
      error
    );
  }
};

/**
 * 3. Cron function to send notification to accounting@premiumpd.com
 * every 2 hours if key status in MongoDB is "Checked In" and Rent Manager status
 * is "Invoiced - Need Keys".
 * Prevents duplicate emails using `isAccountingEmailSent`.
 */
export const checkAccountingInvoicedKeys = async () => {
  console.log(
    "[AccountingInvoicedKeys] Running accounting notification cron job at",
    new Date().toISOString()
  );

  try {
    const checkedInKeys = await Key.find({
      status: "Checked In",
      serviceIssue: { $exists: true, $ne: "", $nin: [null, ""] },
      isDeleted: { $ne: true },
      isAccountingEmailSent: { $ne: true },
    });

    if (!checkedInKeys || checkedInKeys.length === 0) {
      console.log(
        "[AccountingInvoicedKeys] No unsent Checked In keys with serviceIssue found."
      );
      return;
    }

    const headers = await getRMHeaders();
    const baseUrl = process.env.RM_BASE_URL;
    const matchingKeys = [];

    for (const key of checkedInKeys) {
      const serviceIssueId = String(key.serviceIssue || "").trim();
      if (!serviceIssueId) continue;

      try {
        const res = await axios.get(
          `${baseUrl}/ServiceManagerIssues/${serviceIssueId}`,
          {
            headers,
            params: {
              embeds: "Status",
              fields: "Description,Status,StatusID,Title",
            },
            timeout: 10000,
          }
        );

        const rmStatus = res.data?.Status?.Name || res.data?.StatusName || "";
        const rmStatusLower = rmStatus.toLowerCase();

        if (
          rmStatusLower.includes("invoiced - need keys") ||
          rmStatusLower.includes("invoiced – needs keys") ||
          rmStatusLower.includes("invoiced - needs keys")
        ) {
          matchingKeys.push(key);
        }
      } catch (err) {
        console.warn(
          `[AccountingInvoicedKeys] Failed to fetch RM issue ${serviceIssueId} for key ${key._id}:`,
          err.message
        );
      }
    }

    if (matchingKeys.length === 0) {
      console.log(
        "[AccountingInvoicedKeys] No keys matched RM status 'Invoiced - Need Keys'."
      );
      return;
    }

    const recipient = "accounting@premiumpd.com";
    // const recipient = "testrohit1993@gmail.com";
    const subject = `[Key Vault Alert] ${matchingKeys.length} Checked-In Key(s) Ready for Invoiced`;
    const templateName = "template-accountingInvoicedKey";
    const payload = { keys: matchingKeys };

    const ccEmail = process.env.TEST_BCC_EMAIL || "testrohit1993@gmail.com";
    await sendMail(subject, payload, recipient, templateName, ccEmail);

    // Mark sent flag to prevent duplicate emails
    const matchingKeyIds = matchingKeys.map((k) => k._id);
    await Key.updateMany(
      { _id: { $in: matchingKeyIds } },
      {
        $set: {
          isAccountingEmailSent: true,
          accountingEmailSentAt: new Date(),
        },
      }
    );

    console.log(
      `[AccountingInvoicedKeys] Accounting notification email sent successfully to ${recipient} for ${matchingKeys.length} keys.`
    );
  } catch (error) {
    console.error(
      "[AccountingInvoicedKeys] Error in accounting notification cron job:",
      error
    );
  }
};

// Schedule status check cron: 1:30 PM (13:30) America/Los_Angeles
cron.schedule(
  "30 13 * * *",
  async () => {
    await updateKeyStatus();
    await checkAccountingInvoicedKeys();
  },
  {
    timezone: "America/Los_Angeles",
  }
);

// Schedule status check cron: 9:00 PM (21:00) America/Los_Angeles
cron.schedule(
  "0 21 * * *",
  async () => {
    await updateKeyStatus();
    await checkAccountingInvoicedKeys();
  },
  {
    timezone: "America/Los_Angeles",
  }
);

// // Schedule accounting notification cron: Every 2 hours
// // "*/2 * * * *", //every 2 minutes for testing
// cron.schedule(
//   "0 */2 * * *",
//   async () => {
//     await checkAccountingInvoicedKeys();
//   },
//   {
//     timezone: "America/Los_Angeles",
//   }
// );

// Schedule weekly reminder email cron: Every Monday at 8:00 AM
cron.schedule(
  "0 8 * * 1",
  async () => {
    await sendWeeklyKeyReminders();
  },
  {
    timezone: "America/Los_Angeles",
  }
);

export default updateKeyStatus;
