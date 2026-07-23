import axios from "axios";
import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

const apiKey =
  process.env.JOTFORM_API_KEY || "da724b69ac2c6dc23adf791b768e8674";

export default async (req, res, next) => {
  try {
    const payload = req.body;
    //console.log("[Keys-Webhook] Received payload:", JSON.stringify(payload));

    const formID =
      payload?.formID || payload?.formId || payload?.form_id || "Unknown Form";
    const submissionID =
      payload?.submissionID ||
      payload?.submissionId ||
      payload?.submission_id ||
      "Unknown Submission";

    //console.log({ formID }, { submissionID });
    let jotformData = null;
    let answers = {};

    if (!submissionID) {
      console.log(
        "[Webhook-UDF] No submission ID provided in the payload. Skipping JotForm fetch."
      );
    }
    if (submissionID) {
      try {
        // Get submission data from jot form
        const response = await axios.get(
          `https://premiumpd.jotform.com/API/submission/${submissionID}`,
          {
            params: { apiKey },
          }
        );
        // console.log({ response });
        jotformData = response?.data;
        answers = jotformData?.content?.answers || {};
      } catch (error) {
        console.warn(
          `[Webhook-UDF] Failed to fetch Jotform submission ${submissionID}:`,
          error.message
        );
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

    // We want to store ALL data received. We extract key index fields:
    const rfid =
      payload.rfid || payload.rfidNumber || payload.keyId || payload.id || "";
    const communityCode =
      payload.communityCode || payload.community || payload.property || "";
    const unit = payload.unit || payload.unitNumber || "";
    const whoHasIt =
      payload.whoHasIt ||
      payload.requester ||
      payload.holder ||
      payload.vendor ||
      "";
    const serviceRequest = payload.serviceRequest || payload.srNumber || "";
    const serviceStatus =
      payload.serviceStatus ||
      payload.service_status ||
      payload.serviceIssueStatus ||
      payload.service_issue_status ||
      payload.issueStatus ||
      "";

    let status = payload.status || "";
    const serviceCheckText =
      `${serviceStatus} ${status} ${serviceRequest}`.toLowerCase();
    if (
      serviceCheckText.includes("completed") ||
      serviceCheckText.includes("invoiced") ||
      serviceCheckText.includes("withdrawn")
    ) {
      status = "Outstanding";
    } else if (!status || status === "-- None --") {
      status = "Checked Out";
    }

    let types = [];
    if (payload.types) {
      types = Array.isArray(payload.types) ? payload.types : [payload.types];
    } else if (payload.type) {
      types = [payload.type];
    }

    if (!rfid) {
      return res.status(400).json({
        message:
          "RFID (rfid) is required to register checked-out key via webhook.",
      });
    }

    // Check if key already exists
    let key = await Key.findOne({ rfid });
    const logMessage = `${new Date().toLocaleString()}: Webhook received transaction. Holder: ${
      whoHasIt || "Unknown"
    }. Status: ${status}`;

    if (key) {
      // Update existing key
      const oldStatus = key.status;
      key.communityCode = communityCode || key.communityCode;
      key.unit = unit || key.unit;
      if (types.length) key.types = types;
      key.whoHasIt = whoHasIt || key.whoHasIt;
      key.serviceRequest = serviceRequest || key.serviceRequest;
      key.serviceStatus = serviceStatus || key.serviceStatus;
      key.status = status || key.status;
      key.isReturned = key.status === "Checked In";
      if (key.isReturned && !key.returnedAt) key.returnedAt = new Date();
      key.source = "Webhook";
      key.accessLog.push(logMessage);
      key.rawData = jotformData ? { ...req.body, jotformData } : req.body; // Store complete req.body and any fetched Jotform data

      await key.save();
      if (oldStatus !== key.status) {
        sendKeyStatusEmail(key); // send email in background
      }
      return res.json({
        success: true,
        message: "Key updated via webhook",
        key,
      });
    } else {
      // Create new key record
      key = new Key({
        rfid,
        communityCode: communityCode || "UNKNOWN",
        unit: unit || "N/A",
        types: types.length ? types : ["Unit"],
        whoHasIt: whoHasIt || "Unknown (Webhook)",
        serviceRequest: serviceRequest || "",
        serviceStatus: serviceStatus || "",
        status: status || "Checked Out",
        source: "Webhook", //to track the key where it create, from system or webhook
        isReturned: status === "Checked In",
        returnedAt: status === "Checked In" ? new Date() : null,
        accessLog: [logMessage],
        rawData: jotformData ? { ...req.body, jotformData } : req.body, // Store complete req.body
      });

      await key.save();
      sendKeyStatusEmail(key); // send email in background
      return res.json({
        success: true,
        message: "Key created via webhook",
        key,
      });
    }
  } catch (error) {
    console.error("[Keys-Webhook] Error processing:", error);
    next(error);
  }
};
