import cron from "node-cron";
import axios from "axios";
import { Key } from "../models/index.js";
import { getRMHeaders } from "../utils/rentManager.js";

/**
 * Helper to fetch ServiceManagerIssue data from Rent Manager.
 * Prioritizes /ServiceManagerIssues?PageSize=10&PageNumber={serviceIssueId}
 * with fallbacks for direct ID endpoint and filter expressions.
 */
async function fetchRMServiceManagerIssue(serviceIssueId, headers) {
  const baseUrl = process.env.RM_BASE_URL;
  if (!baseUrl) return null;

  // Attempt 1: Endpoint format /ServiceManagerIssues?PageSize=10&PageNumber={serviceIssueId}
  try {
    const res = await axios.get(`${baseUrl}/ServiceManagerIssues`, {
      headers,
      params: { PageSize: 10, PageNumber: serviceIssueId, embeds: "Status" },
      timeout: 10000,
    });
    if (res.data) {
      const items = Array.isArray(res.data) ? res.data : [res.data];
      const match = items.find(
        (item) => String(item?.ServiceManagerIssueID) === String(serviceIssueId)
      );
      if (match) return match;
      if (items[0]) return items[0];
    }
  } catch (err) {
    // Continue to next attempt
  }

  // Attempt 2: Direct endpoint /ServiceManagerIssues/{serviceIssueId}
  try {
    const res = await axios.get(
      `${baseUrl}/ServiceManagerIssues/${serviceIssueId}`,
      { headers, params: { embeds: "Status" }, timeout: 10000 }
    );
    if (res.data) {
      const data = Array.isArray(res.data) ? res.data[0] : res.data;
      if (data) return data;
    }
  } catch (err) {
    // Continue to next attempt
  }

  // Attempt 3: Query with filter expression (ServiceManagerIssueID,eq,{serviceIssueId})
  try {
    const res = await axios.get(`${baseUrl}/ServiceManagerIssues`, {
      headers,
      params: { filters: `ServiceManagerIssueID,eq,${serviceIssueId}`, embeds: "Status" },
      timeout: 10000,
    });
    if (res.data) {
      const items = Array.isArray(res.data) ? res.data : [res.data];
      const match = items.find(
        (item) => String(item?.ServiceManagerIssueID) === String(serviceIssueId)
      );
      if (match) return match;
      if (items[0]) return items[0];
    }
  } catch (err) {
    console.warn(
      `[UpdateKeyStatus] Failed to fetch RM issue ${serviceIssueId}:`,
      err.message
    );
  }

  return null;
}

/**
 * Extracts status string from Rent Manager ServiceManagerIssue response.
 */
function extractStatus(issueData) {
  if (!issueData) return null;

  if (typeof issueData.Status === "string") return issueData.Status;
  if (issueData.Status && typeof issueData.Status.Name === "string")
    return issueData.Status.Name;
  if (issueData.Status && typeof issueData.Status.Status === "string")
    return issueData.Status.Status;
  if (typeof issueData.StatusName === "string") return issueData.StatusName;
  if (typeof issueData.StatusType === "string") return issueData.StatusType;
  if (typeof issueData.ServiceManagerIssueStatus === "string")
    return issueData.ServiceManagerIssueStatus;
  if (issueData.ServiceManagerIssueStatus && typeof issueData.ServiceManagerIssueStatus.Name === "string")
    return issueData.ServiceManagerIssueStatus.Name;
  if (typeof issueData.status === "string") return issueData.status;

  return null;
}

/**
 * Main cron function to update key statuses from Rent Manager ServiceManagerIssues.
 */
export const updateKeyStatus = async () => {
  console.log(
    "[UpdateKeyStatus] Running key status update cron job at",
    new Date().toISOString()
  );

  try {
    // 1. Get all keys where serviceIssue exists in MongoDB
    const keysWithServiceIssue = await Key.find({
      serviceIssue: { $exists: true, $ne: "", $nin: [null, ""] },
      isDeleted: { $ne: true },
    });

    if (!keysWithServiceIssue || keysWithServiceIssue.length === 0) {
      console.log("[UpdateKeyStatus] No keys found with serviceIssue.");
      return;
    }

    console.log(
      `[UpdateKeyStatus] Found ${keysWithServiceIssue.length} keys with serviceIssue to check.`
    );

    const headers = await getRMHeaders();
    let updatedCount = 0;

    for (const key of keysWithServiceIssue) {
      const serviceIssueId = String(key.serviceIssue || "").trim();
      if (!serviceIssueId) continue;

      const rmData = await fetchRMServiceManagerIssue(serviceIssueId, headers);
      if (!rmData) continue;

      const rmStatus = extractStatus(rmData);
      if (!rmStatus) continue;

      const newStatus = rmStatus.trim();
      const currentStatus = String(key.status || "").trim();

      // 2. Check if rentmanager status != keys status
      if (newStatus !== currentStatus) {
        console.log(
          `[UpdateKeyStatus] Key ${key._id} (ServiceIssue: ${serviceIssueId}): Updating status from "${currentStatus}" to "${newStatus}"`
        );

        const logMsg = `${new Date().toLocaleString()}: Status updated via RM cron from "${currentStatus}" to "${newStatus}" (ServiceIssue: ${serviceIssueId})`;

        key.status = newStatus;
        if (newStatus === "Checked In") {
          key.isReturned = true;
          if (!key.returnedAt) key.returnedAt = new Date();
        }
        if (key.rawData && typeof key.rawData === "object") {
          key.rawData.status = newStatus;
        }
        if (!Array.isArray(key.accessLog)) {
          key.accessLog = [];
        }
        key.accessLog.push(logMsg);

        try {
          await key.save();
        } catch (saveErr) {
          // If Mongoose schema enum validation fails on key.save(), fallback to direct update
          await Key.updateOne(
            { _id: key._id },
            {
              $set: {
                status: newStatus,
                isReturned: newStatus === "Checked In",
                returnedAt:
                  newStatus === "Checked In"
                    ? key.returnedAt || new Date()
                    : key.returnedAt,
                ...(key.rawData ? { "rawData.status": newStatus } : {}),
              },
              $push: { accessLog: logMsg },
            }
          );
        }
        updatedCount++;
      }
    }

    console.log(
      `[UpdateKeyStatus] Key status update job finished. Updated ${updatedCount} keys.`
    );
  } catch (error) {
    console.error("[UpdateKeyStatus] Fatal error in key status update cron job:", error);
  }
};

// Schedule cron to run every 2 hours
cron.schedule(
  "0 */2 * * *",
  async () => {
    await updateKeyStatus();
  },
  {
    timezone: "America/Los_Angeles",
  }
);

export default updateKeyStatus;
