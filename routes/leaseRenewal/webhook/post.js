import axios from "axios";
import multer from "multer";
import { getRMHeaders } from "../../../utils/rentManager.js";

const upload = multer();

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

  if (typeof dateVal === "object") {
    const { year, month, day } = dateVal;
    if (year && month && day) {
      return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
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

    const formID =
      payload?.formID || payload?.formId || payload?.form_id || "Unknown Form";
    const submissionID =
      payload?.submissionId ||
      payload?.submissionID ||
      payload?.submission_id ||
      "Unknown Submission";

    let jotformData = payload.jotformData || null;
    let answers = payload?.content?.answers || payload?.answers || {};

    if (!answers || Object.keys(answers).length === 0) {
      if (submissionID && submissionID !== "Unknown Submission") {
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
    }

    const unitId = getAnswerByName(answers, "UnitID") || "";
    const leaseExpirationDate =
      getAnswerByName(answers, "Lease Expiration Date") || "";
    console.log({ unitId, leaseExpirationDate });

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
        `[LeaseRenewal] Fetched ${leases?.length || 0} leases for UnitID ${unitId}`
      );

      if (Array.isArray(leases) && leases.length > 0) {
        // 2. Get updated lease from array by comparing MoveInDate (greatest MoveInDate)
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
          `[LeaseRenewal] Selected LeaseID ${selectedLease?.LeaseID} with MoveInDate ${selectedLease?.MoveInDate}`
        );

        // 3. Create payload for new lease renewal
        const currentDateStr = formatDateToYYYYMMDD(new Date());
        const formattedExpirationDate =
          formatDateToYYYYMMDD(leaseExpirationDate) || leaseExpirationDate;

        const leaseRenewalPayload = {
          ParentLeaseID: selectedLease.LeaseID,
          UnitID: isNaN(Number(unitId)) ? unitId : Number(unitId),
          StartDate: currentDateStr,
          EndDate: formattedExpirationDate,
        };

        console.log(
          "[LeaseRenewal] Creating lease renewal with payload:",
          leaseRenewalPayload
        );

        // Create lease renewal API: /LeaseRenewals
        const renewalRes = await axios.post(
          `${baseUrl}/LeaseRenewals`,
          [leaseRenewalPayload],
          { headers }
        );

        leaseRenewalResult = renewalRes?.data;
        console.log(
          "[LeaseRenewal] Lease renewal created successfully:",
          leaseRenewalResult
        );
      } else {
        console.warn(`[LeaseRenewal] No leases found for UnitID ${unitId}`);
      }
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
    console.error(
      "[WEBHOOK ERROR] Error handling lease renewal webhook:",
      error
    );
    return res.status(500).json({
      success: false,
      message: "Error processing lease renewal webhook",
      error: error.message,
    });
  }
};

