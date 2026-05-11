import cron from "node-cron";
import { JotformSubmission, Upload } from "../models/index.js";
import sendMail from "../methods/sendMail.js";
import ExcelJS from "exceljs";

const getStatusPriority = (status) => {
  const priorities = {
    Failed: 1,
    Pending: 2,
    Uploading: 3,
    Uploaded: 4,
  };
  return priorities[status] || 99;
};

const testBccEmail = process.env.TEST_BCC_EMAIL || "testrohit1993@gmail.com";

const sendDailyUploadReport = async () => {
  console.log(
    "[DailyUploadReport] Running daily report generation at",
    new Date().toISOString()
  );

  try {
    const now = new Date();
    const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    // 1. Find all submissions from the previous 24 hours
    const recentSubmissions = await JotformSubmission.find({
      createdAt: { $gte: twentyFourHoursAgo },
    });

    const recentSubIds = recentSubmissions.map((s) => s.submissionId);

    if (recentSubIds.length === 0) {
      console.log("[DailyUploadReport] No submissions to report today.");
      return;
    }

    // 5. Group by Reply Email
    const groupedReports = {};
    console.log("Group by Reply Email", { recentSubIds });

    for (const submission of recentSubmissions) {
      const replyEmail = submission.replyEmail;
      console.log({ replyEmail });
      if (!replyEmail) continue;

      if (!groupedReports[replyEmail]) {
        groupedReports[replyEmail] = [];
      }

      // Fetch uploads for this submission
      const uploads = await Upload.find({
        submissionId: submission.submissionId,
      });

      const totalFiles = uploads.length;
      const successFiles = uploads.filter(
        (u) => u.status === "Uploaded"
      ).length;
      const failedFiles = uploads.filter((u) => u.status === "Failed").length;
      const uploadingFiles = uploads.filter(
        (u) => u.status === "Uploading"
      ).length;
      const pendingFiles = uploads.filter((u) => u.status === "Pending").length;

      // Determine overall Status
      let overallStatus = "Uploaded";
      if (failedFiles > 0) overallStatus = "Failed";
      else if (pendingFiles > 0) overallStatus = "Pending";
      else if (uploadingFiles > 0) overallStatus = "Uploading";
      else if (totalFiles === 0) overallStatus = "Pending";

      const reactAppUrl =
        process.env.REACT_APP_URL || "https://app.premiumpd.com";

      groupedReports[replyEmail].push({
        status: overallStatus,
        formId: submission.formId,
        formName: submission.formName || "N/A",
        totalFiles,
        successFiles,
        failedFiles,
        uploadingFiles: uploadingFiles + pendingFiles, // Combine for display if needed, or keep separate
        submissionId: submission.submissionId,
        residentName:
          (submission.answers &&
            Object.values(submission.answers).find(
              (item) => item?.text === "User Name"
            )?.answer) ||
          "N/A",
        date: new Date(
          submission.createdAt || submission.updatedAt
        ).toLocaleString(), // using toLocaleString to get both date and time
        uploadUrl: `${reactAppUrl}/jotform/form/${submission.formId}?submissionId=${submission.submissionId}`,
      });
    }

    // console.log({ groupedReports });
    // 6. Generate Excel and Send the emails
    for (const [email, submissions] of Object.entries(groupedReports)) {
      if (submissions.length === 0) continue;

      // Sort submissions: Failed, Pending, Uploading, Uploaded
      submissions.sort(
        (a, b) => getStatusPriority(a.status) - getStatusPriority(b.status)
      );

      try {
        const workbook = new ExcelJS.Workbook();
        const sheet = workbook.addWorksheet("Upload Report");

        // Add Header Row
        sheet.addRow([
          "Status",
          "Submission Date",
          "FormId",
          "Form Name",
          "Total Files",
          "Success Files",
          "Failed Files",
          "Uploading Files",
          "Submission ID",
          "Upload URL",
        ]);

        // Style Header
        sheet.getRow(1).font = { bold: true };
        sheet.getRow(1).fill = {
          type: "pattern",
          pattern: "solid",
          fgColor: { argb: "FFD3D3D3" },
        };

        // Add Data Rows
        submissions.forEach((sub) => {
          const row = sheet.addRow([
            sub.status,
            sub.date, // Submission Date
            sub.formId, // Placeholder for FormId
            sub.formName,
            sub.totalFiles,
            sub.successFiles,
            sub.failedFiles,
            sub.uploadingFiles,
            sub.submissionId, // Placeholder for SubmissionId
            sub.uploadUrl, // Upload URL
          ]);

          // Force FormId and SubmissionId to be strings to avoid scientific notation
          const formIdCell = row.getCell(3);
          formIdCell.value = String(sub.formId);
          formIdCell.numFmt = "@";

          const subIdCell = row.getCell(9);
          subIdCell.value = String(sub.submissionId);
          subIdCell.numFmt = "@";

          // Color coding Status column
          const statusCell = row.getCell(1);
          if (sub.status === "Failed")
            statusCell.font = { color: { argb: "FFFF0000" }, bold: true };
          if (sub.status === "Uploaded")
            statusCell.font = { color: { argb: "FF008000" } };
          if (sub.status === "Uploading" || sub.status === "Pending")
            statusCell.font = { color: { argb: "FFFFA500" } };
        });

        // Auto-fit columns (approximate)
        sheet.columns.forEach((column) => {
          column.width = 20;
        });

        const buffer = await workbook.xlsx.writeBuffer();

        const completed = submissions.filter((s) => s.status === "Uploaded");
        const notCompleted = submissions.filter((s) => s.status !== "Uploaded");

        const payload = {
          dateStr: now.toLocaleDateString(),
          completed,
          notCompleted,
        };

        const subject = `Daily Upload Report - ${payload.dateStr}`;

        await sendMail(
          subject,
          payload,
          email,
          "dailyUploadReport",
          "",
          [
            {
              filename: `Daily_Upload_Report_${
                now.toISOString().split("T")[0]
              }.xlsx`,
              content: buffer,
            },
          ],
          "",
          [testBccEmail]
        );

        console.log(`[DailyUploadReport] Sent Excel report to ${email}`);
      } catch (err) {
        console.error(
          `[DailyUploadReport] Failed to send report to ${email}`,
          err
        );
      }
    }

    console.log("[DailyUploadReport] Successfully finished report generation.");
  } catch (error) {
    console.error("[DailyUploadReport] Fatal error:", error);
  }
};

// Schedule to run every day at 7:00 AM Pacific Time
cron.schedule(
  "0 7 * * *",
  // "*/2 * * * *", //every 2 mins for testing
  async () => {
    await sendDailyUploadReport();
  },
  {
    timezone: "America/Los_Angeles",
  }
);

export default sendDailyUploadReport;
