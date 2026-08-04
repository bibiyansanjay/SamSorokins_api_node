import axios from "axios";
import { Key } from "../../models";
// import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

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

// Non-Vendor Webhook Handler (/api/keys/webhook/non-vendor)
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
          "[Keys-NonVendor-Webhook] Failed to parse rawRequest:",
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
            `[Keys-NonVendor-Webhook] Failed to fetch Jotform submission ${submissionID}:`,
            error.message
          );
        }
      }
    }

    console.log(
      "Received Non-Vendor webhook data from JotForm",
      jotformData,
      "submissionId:",
      submissionID,
      "answers:",
      answers,
      "formID:",
      formID
    );

    // Extract Non-Vendor fields
    const name = getAnswerByName(answers, "Name") || "";
    const vendor = name || "Unknown Requester";

    const phoneNumber =
      getAnswerByName(answers, "Phone Number") || payload.phoneNumber || "";

    const email = getAnswerByName(answers, "Email") || "";

    const property = getAnswerByName(answers, "Property") || "UNKNOWN";

    const purpose = getAnswerByName(answers, "Purpose") || "";
    const purposeDescription =
      getAnswerByName(answers, "Purpose Description") || "";

    const rawPickUp = getAnswerByName(answers, "Pick Up Date & Time") || null;
    const pickUpDateTime = formatPickUpDate(rawPickUp);

    const keysNeeded = getAnswerByName(answers, "Keys Needed") || "";

    const areKeysForYou =
      getAnswerByName(answers, "Are the Keys for You?") || "";

    const whoWillPickUp = getAnswerByName(answers, "whoWillPickUp") || "";
    // getAnswerByName(answers, "Who will pick them up?") || "";

    const pickerPhoneNumber =
      getAnswerByName(answers, "pickerPhoneNumber") || "";
    // getAnswerByName(answers, "phoneNumber") || "";
    const pickerEmail = getAnswerByName(answers, "pickerEmail") || "";

    const willBeReturned =
      getAnswerByName(answers, "Will they be returned?") ||
      getAnswerByName(answers, "willThey") ||
      "";

    const whyNotReturned =
      getAnswerByName(answers, "Why?") || getAnswerByName(answers, "why") || "";

    let status = payload.status || "Requested";
    const logMessage = `${new Date().toLocaleString()}: New key request created via Non-Vendor Webhook. Requester: ${vendor}, Property: ${property}`;

    const newKey = new Key({
      vendor,
      phoneNumber,
      email,
      property,
      serviceIssue: "",
      fullAddress: "",
      pickUpDateTime,
      byWhen: "",
      keysNeeded,
      status,
      // requestType: "Non-Vendor",
      userType: "Non-Vendor",
      purpose,
      purposeDescription,
      areKeysForYou,
      whoWillPickUp,
      pickerPhoneNumber,
      pickerEmail,
      willBeReturned,
      whyNotReturned,
      source: "Webhook",
      isReturned: status === "Checked In",
      returnedAt: status === "Checked In" ? new Date() : null,
      accessLog: [logMessage],
      rawData: jotformData ? { ...req.body, jotformData } : req.body,
    });

    await newKey.save();
    // sendKeyStatusEmail(newKey); // send email in background

    return res.json({
      success: true,
      message: "Non-Vendor key request created successfully via webhook",
      key: newKey,
    });
  } catch (error) {
    console.error("[Keys-NonVendor-Webhook] Error processing:", error);
    next(error);
  }
};
