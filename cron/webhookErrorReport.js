import cron from "node-cron";
import { WebhookErrorLog } from "../models/index.js";
import sendMail from "../methods/sendMail.js";
import ExcelJS from "exceljs";

const testBccEmail = process.env.TEST_BCC_EMAIL || "testrohit1993@gmail.com";
const errorLogEmail = process.env.ERROR_LOG_EMAIL || "turnovers@premiumpd.com";
// const errorLogEmail = "bibiyan@yopmail.com";

const sendWebhookErrorReport = async () => {
  console.log(
    "[WebhookErrorReport] Running daily report generation at",
    new Date().toISOString()
  );

  try {
    const now = new Date();
    const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    // 1. Find all error logs from the previous 24 hours
    const errorLogs = await WebhookErrorLog.find({
      createdAt: { $gte: twentyFourHoursAgo },
    }).sort({ createdAt: -1 });

    if (errorLogs.length === 0) {
      console.log("[WebhookErrorReport] No error logs to report today.");
      return;
    }

    console.log(`[WebhookErrorReport] Found ${errorLogs.length} error logs.`);

    // 2. Generate Excel
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet("Webhook Error Report");

    // Add Header Row
    sheet.addRow([
      "Date (ISO)",
      "Email",
      "Form ID",
      "Submission ID",
      "Error Type",
      "Reason",
      // "Details",
    ]);

    // Style Header
    sheet.getRow(1).font = { bold: true };
    sheet.getRow(1).fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFD3D3D3" },
    };

    // Add Data Rows
    errorLogs.forEach((log) => {
      const row = sheet.addRow([
        log.createdAt ? log.createdAt.toISOString() : "N/A",
        log.email || "N/A",
        log.formId || "N/A",
        log.submissionID || "N/A",
        log.type || "N/A",
        log.reason || "N/A",
        // log.details ? JSON.stringify(log.details) : "N/A",
      ]);

      // Ensure formId and submissionId are strings
      const formIdCell = row.getCell(3);
      formIdCell.value = String(log.formId || "");
      formIdCell.numFmt = "@";

      const subIdCell = row.getCell(4);
      subIdCell.value = String(log.submissionID || "");
      subIdCell.numFmt = "@";
    });

    // Auto-fit columns (approximate)
    sheet.columns.forEach((column) => {
      column.width = 25;
    });

    const buffer = await workbook.xlsx.writeBuffer();

    const dateStr = now.toLocaleDateString();
    const subject = `Daily Webhook Error Report - ${dateStr}`;

    const payload = {
      dateStr,
      errorCount: errorLogs.length,
    };

    // 3. Send Email
    await sendMail(
      subject,
      payload,
      errorLogEmail,
      "webhookErrorReport", // Template name (we might need to create it or use a generic one)
      "",
      [
        {
          filename: `Webhook_Error_Report_${
            now.toISOString().split("T")[0]
          }.xlsx`,
          content: buffer,
        },
      ],
      "",
      [testBccEmail]
    );

    console.log(
      `[WebhookErrorReport] Sent Excel report to ${errorLogEmail} and BCC ${testBccEmail}`
    );
  } catch (error) {
    console.error("[WebhookErrorReport] Fatal error:", error);
  }
};

// Schedule to run every day at 8:00 AM Pacific Time
cron.schedule(
  "0 8 * * *",
  // "*/2 * * * *", //every 2 mins for testing
  async () => {
    await sendWebhookErrorReport();
  },
  {
    timezone: "America/Los_Angeles",
  }
);

export default sendWebhookErrorReport;
