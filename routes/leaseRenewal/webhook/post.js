import axios from "axios";
import { getRMHeaders } from "../../../utils/rentManager.js";

/**
 * @desc    Lease Renewal Webhook POST Handler
 * @route   POST /leaseRenewal/webhook
 * @access  Public
 */
const apiKey =
  process.env.JOTFORM_API_KEY || "da724b69ac2c6dc23adf791b768e8674";

/**
 * Helper to get exact answer value by matching field's 'text' or 'name'.
 */
function getAnswerByName(answers, fieldName, keyName = "answer") {
  if (!answers) return null;
  const target = (fieldName || "").trim().toLowerCase();

  const field = Object.values(answers).find((item) => {
    if (!item) return false;
    const text = (item.text || "").trim().toLowerCase();
    const name = (item.name || "").trim().toLowerCase();
    return text === target || name === target;
  });

  if (!field) return null;

  let value = field?.[keyName] ?? field?.prettyFormat ?? null;

  if (value && typeof value === "object") {
    if (value.prettyFormat) return value.prettyFormat;
    if (value.full) return value.full;
    if (value.datetime) return value.datetime;
  }

  if (typeof value === "string") {
    return value.replace(/<[^>]*>/g, "").trim();
  }

  return value;
}

/**
 * Helper to format date strings or objects to YYYY-MM-DD format.
 */
function formatDateToYYYYMMDD(dateVal) {
  if (!dateVal) return null;

  if (dateVal instanceof Date) {
    if (isNaN(dateVal.getTime())) return null;
    const year = dateVal.getFullYear();
    const month = String(dateVal.getMonth() + 1).padStart(2, "0");
    const day = String(dateVal.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }

  if (typeof dateVal === "object") {
    const { year, month, day } = dateVal;
    if (year && month && day) {
      return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(
        2,
        "0"
      )}`;
    }
  }

  const s = String(dateVal).trim();

  // YYYY-MM-DD
  const ymdMatch = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
  if (ymdMatch) {
    const [_, year, month, day] = ymdMatch;
    return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  }

  // MM/DD/YYYY or MM-DD-YYYY
  const mdyMatch = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})/);
  if (mdyMatch) {
    const [_, month, day, year] = mdyMatch;
    return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  }

  const d = new Date(s);
  if (!isNaN(d.getTime())) {
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }

  return s;
}

/**
 * Helper to calculate StartDate (+1 day) and EndDate (+1 year) from leaseExpirationDate.
 */
function calculateRenewalDates(leaseExpirationDate) {
  const formattedExpStr = formatDateToYYYYMMDD(leaseExpirationDate);

  let startDateStr = formatDateToYYYYMMDD(new Date());
  let endDateStr = formattedExpStr || leaseExpirationDate;

  if (formattedExpStr && /^(\d{4})-(\d{2})-(\d{2})$/.test(formattedExpStr)) {
    const [year, month, day] = formattedExpStr.split("-").map(Number);

    // StartDate: plus 1 day in Lease Expiration Date
    const startDateObj = new Date(year, month - 1, day + 1);
    startDateStr = formatDateToYYYYMMDD(startDateObj);

    // EndDate: plus 1 year in Lease Expiration Date
    const endDateObj = new Date(year + 1, month - 1, day);
    endDateStr = formatDateToYYYYMMDD(endDateObj);
  }

  return { startDateStr, endDateStr };
}

export default async (req, res, next) => {
  try {
    let payload = req.body;

    if (req.body.rawRequest) {
      try {
        const raw =
          typeof req.body.rawRequest === "string"
            ? JSON.parse(req.body.rawRequest)
            : req.body.rawRequest;
        payload = { ...payload, ...raw };
      } catch (e) {
        console.warn(
          "[LeaseRenewal-Webhook] Failed to parse rawRequest:",
          e.message
        );
      }
    }

    const submissionID =
      payload?.submissionId || payload?.submissionID || payload?.submission_id;

    console.log(
      "------------------------- Lease Renewal Webhook Received -------------------------",
      submissionID
    );

    if (!submissionID) {
      console.warn(
        "[LeaseRenewal-Webhook] No submission ID found in request payload. Aborting processing."
      );
      return res.status(400).json({
        success: false,
        message: "No submission ID found in request payload",
      });
    }

    let jotformData = payload.jotformData || null;
    let answers = payload?.content?.answers || payload?.answers || {};

    if (!answers || Object.keys(answers).length === 0) {
      try {
        const response = await axios.get(
          `https://premiumpd.jotform.com/API/submission/${submissionID}`,
          {
            params: { apiKey },
          }
        );
        jotformData = response?.data;
        answers = jotformData?.content?.answers || {};
      } catch (error) {
        console.warn(
          `[LeaseRenewal-Webhook] Failed to fetch Jotform submission ${submissionID}:`,
          error.message
        );
      }
    }

    if (!answers || Object.keys(answers).length === 0) {
      console.warn(
        `[LeaseRenewal-Webhook] No submission answers found for Submission ID ${submissionID}. Aborting processing.`
      );
      return res.status(400).json({
        success: false,
        message: `No submission answers found for Submission ID ${submissionID}`,
      });
    }

    const unitId = getAnswerByName(answers, "UnitID") || "";
    const leaseExpirationDate =
      getAnswerByName(answers, "Lease Expiration Date") || "";

    console.log(
      `[LeaseRenewal] Extracted UnitID: "${unitId}", Lease Expiration Date: "${leaseExpirationDate}"`
    );

    let leaseRenewalResult = null;
    let selectedLease = null;

    if (unitId) {
      const headers = await getRMHeaders();
      const baseUrl = process.env.RM_BASE_URL;

      // 1. Get lease with unit id: /Leases?filters=UnitID,eq,unitId
      const leasesRes = await axios.get(
        `${baseUrl}/Leases?filters=UnitID,eq,${unitId}`,
        { headers }
      );

      const leases = leasesRes?.data;
      console.log(
        `[LeaseRenewal] Fetched ${
          leases?.length || 0
        } lease(s) from RentManager for UnitID: ${unitId}`
      );

      if (Array.isArray(leases) && leases.length > 0) {
        // Log summary of all candidate leases found
        const candidateSummary = leases.map((l) => ({
          LeaseID: l.LeaseID,
          UnitID: l.UnitID,
          MoveInDate: l.MoveInDate,
          StartDate: l.StartDate,
          EndDate: l.EndDate,
        }));
        console.log(
          "[LeaseRenewal] All Candidate Leases Found for UnitID:",
          candidateSummary
        );

        // 2. Select lease by greatest MoveInDate
        selectedLease = leases.reduce((prev, current) => {
          const prevTime = prev?.MoveInDate
            ? new Date(prev.MoveInDate).getTime()
            : 0;
          const currentTime = current?.MoveInDate
            ? new Date(current.MoveInDate).getTime()
            : 0;
          return currentTime > prevTime ? current : prev;
        }, leases[0]);

        console.log(
          `[LeaseRenewal] Selected Target Lease -> LeaseID: ${selectedLease?.LeaseID}, MoveInDate: ${selectedLease?.MoveInDate}, Current EndDate: ${selectedLease?.EndDate}`
        );

        // 3. Create payload for new lease renewal
        const { startDateStr, endDateStr } =
          calculateRenewalDates(leaseExpirationDate);

        const leaseRenewalPayload = {
          ParentLeaseID: selectedLease.LeaseID,
          UnitID: isNaN(Number(unitId)) ? unitId : Number(unitId),
          StartDate: startDateStr,
          EndDate: endDateStr,
        };

        console.log(
          "[LeaseRenewal] Submitting Lease Renewal Payload:",
          JSON.stringify(leaseRenewalPayload, null, 2)
        );

        // Create lease renewal API: /LeaseRenewals
        const renewalRes = await axios.post(
          `${baseUrl}/LeaseRenewals`,
          [leaseRenewalPayload],
          { headers }
        );

        leaseRenewalResult = renewalRes?.data;
        console.log(
          "[LeaseRenewal] SUCCESS: Lease renewal created successfully:",
          leaseRenewalResult
        );
      } else {
        console.warn(
          `[LeaseRenewal] WARNING: No leases found in RentManager for UnitID: ${unitId}`
        );
      }
    } else {
      console.warn(
        "[LeaseRenewal] WARNING: UnitID was not provided in Jotform submission answers."
      );
    }

    return res.status(200).json({
      success: true,
      message: "Lease renewal webhook processed successfully",
      timestamp: new Date().toISOString(),
      unitId,
      leaseExpirationDate,
      selectedLease,
      leaseRenewalResult,
      receivedData: req.body,
    });
  } catch (error) {
    const errorData = error?.response?.data || error?.message;
    console.error(
      "[WEBHOOK ERROR] Error handling lease renewal webhook:",
      errorData
    );

    return res.status(500).json({
      success: false,
      message: "Error processing lease renewal webhook",
      error: error.message,
    });
  }
};
