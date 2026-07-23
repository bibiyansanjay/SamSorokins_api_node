import axios from "axios";
import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

const apiKey =
  process.env.JOTFORM_API_KEY || "da724b69ac2c6dc23adf791b768e8674";

/**
 * Helper to get exact answer value by matching field's 'text' or 'name'.
 */
function getAnswerByName(answers, fieldName, keyName = "answer") {
  if (!answers) return null;
  const field = Object.values(answers).find(
    (item) => item && (item.text === fieldName || item.name === fieldName)
  );
  if (!field) return null;

  const value = field?.[keyName] || null;

  if (typeof value === "string") {
    return value.replace(/<[^>]*>/g, "").trim();
  }

  return value;
}

/**
 * Formats Pick Up Date & Time into 'DD-MM-YYYY HH:MM:SS' format.
 */
function formatPickUpDate(pickUpVal) {
  if (!pickUpVal) return "";

  if (typeof pickUpVal === "object") {
    if (pickUpVal.datetime && typeof pickUpVal.datetime === "string") {
      const parts = pickUpVal.datetime.split(" ");
      if (parts.length >= 2) {
        const dateParts = parts[0].split("-"); // [YYYY, MM, DD]
        if (dateParts.length === 3) {
          const timeStr = parts[1].length === 5 ? `${parts[1]}:00` : parts[1];
          return `${dateParts[2].padStart(2, "0")}-${dateParts[1].padStart(
            2,
            "0"
          )}-${dateParts[0]} ${timeStr}`;
        }
      }
    }
    const day = pickUpVal.day ? String(pickUpVal.day).padStart(2, "0") : "01";
    const month = pickUpVal.month
      ? String(pickUpVal.month).padStart(2, "0")
      : "01";
    const year = pickUpVal.year ? String(pickUpVal.year) : "1970";
    let hourInt = parseInt(pickUpVal.hour || "0", 10);
    const minStr = pickUpVal.min
      ? String(pickUpVal.min).padStart(2, "0")
      : "00";
    const ampm = (pickUpVal.ampm || "").toUpperCase();

    if (ampm === "PM" && hourInt < 12) hourInt += 12;
    if (ampm === "AM" && hourInt === 12) hourInt = 0;
    const hourStr = String(hourInt).padStart(2, "0");

    return `${day}-${month}-${year} ${hourStr}:${minStr}:00`;
  }

  if (typeof pickUpVal === "string") {
    const isoMatch = pickUpVal.match(
      /^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2}):?(\d{2})?/
    );
    if (isoMatch) {
      const [, y, m, d, hh, mm, ss] = isoMatch;
      return `${d}-${m}-${y} ${hh}:${mm}:${ss || "00"}`;
    }

    const d = new Date(pickUpVal);
    if (!isNaN(d.getTime())) {
      const day = String(d.getDate()).padStart(2, "0");
      const month = String(d.getMonth() + 1).padStart(2, "0");
      const year = d.getFullYear();
      const hours = String(d.getHours()).padStart(2, "0");
      const mins = String(d.getMinutes()).padStart(2, "0");
      const secs = String(d.getSeconds()).padStart(2, "0");
      return `${day}-${month}-${year} ${hours}:${mins}:${secs}`;
    }
  }

  return String(pickUpVal);
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
        console.warn("[Keys-Webhook] Failed to parse rawRequest:", e.message);
      }
    }

    const formID =
      payload?.formID || payload?.formId || payload?.form_id || "Unknown Form";
    const submissionID =
      payload?.submissionID ||
      payload?.submissionId ||
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
            `[Keys-Webhook] Failed to fetch Jotform submission ${submissionID}:`,
            error.message
          );
        }
      }
    }

    console.log(
      "Received data from jot form",
      jotformData,
      "submissionId:",
      submissionID,
      "answers:",
      answers,
      "formID:",
      formID
    );

    // Extract exact values from Jotform answers using getAnswerByName with fallback to payload
    const vendor = getAnswerByName(answers, "Vendor");
    // ||getAnswerByName(answers, "Vendor Name") ||
    // payload.vendor ||
    // payload.whoHasIt ||
    ("Unknown Vendor");

    const phoneNumber = getAnswerByName(answers, "Phone Number");
    // ||
    // payload.phoneNumber ||
    // payload.phone ||
    ("");

    const email = getAnswerByName(answers, "Email");
    //  ||
    // getAnswerByName(answers, "Repairs Email") ||
    // payload.email ||
    ("");

    const property = getAnswerByName(answers, "Property");
    //  ||
    // payload.property ||
    // payload.communityCode ||
    ("UNKNOWN");

    const serviceIssue = getAnswerByName(answers, "Service Issue #");
    // ||
    // payload.serviceIssue ||
    // payload.serviceRequest ||
    ("");

    const fullAddress = getAnswerByName(answers, "Full Address");
    // ||
    // payload.fullAddress ||
    // payload.address ||
    ("");

    const rawPickUp = getAnswerByName(answers, "Pick Up Date & Time");
    // ||
    // payload.pickUpDateTime ||
    // payload.pickUp;

    const pickUpDateTime = formatPickUpDate(rawPickUp);

    const byWhen =
      getAnswerByName(answers, "By When") ||
      // || payload.byWhen
      "";

    const keysNeeded =
      getAnswerByName(answers, "Keys Needed") ||
      // || payload.keysNeeded
      "";

    // Requirement 1: Every webhook creates a new request.
    let rfid = payload.rfid || payload.rfidNumber || payload.keyId || "";
    if (!rfid) {
      rfid =
        submissionID && submissionID !== "Unknown Submission"
          ? `REQ-${submissionID}`
          : `REQ-${Date.now()}-${Math.floor(1000 + Math.random() * 9000)}`;
    }

    // Ensure unique RFID per request entry in database
    const existingRfid = await Key.findOne({ rfid });
    if (existingRfid) {
      rfid = `${rfid}-${Date.now()}`;
    }

    let status = payload.status || "Key Requested";
    const whoHasIt = vendor;
    const communityCode = property;
    const serviceRequest = serviceIssue;

    const logMessage = `${new Date().toLocaleString()}: New key request created via webhook. Vendor: ${vendor}, Property: ${property}`;

    const newKey = new Key({
      rfid,
      communityCode,
      unit: payload.unit || payload.unitNumber || "",
      types: keysNeeded ? [keysNeeded] : ["Unit"],
      whoHasIt,
      serviceRequest,
      serviceStatus: payload.serviceStatus || "",
      status,
      source: "Webhook",
      isReturned: status === "Checked In",
      returnedAt: status === "Checked In" ? new Date() : null,
      accessLog: [logMessage],
      vendor,
      phoneNumber,
      email,
      property,
      serviceIssue,
      fullAddress,
      pickUpDateTime,
      byWhen,
      keysNeeded,
      rawData: jotformData ? { ...req.body, jotformData } : req.body,
    });

    await newKey.save();
    sendKeyStatusEmail(newKey); // send email in background

    return res.json({
      success: true,
      message: "Key request created successfully via webhook",
      key: newKey,
    });
  } catch (error) {
    console.error("[Keys-Webhook] Error processing:", error);
    next(error);
  }
};
