import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

export default async (req, res, next) => {
  try {
    const {
      vendor,
      whoHasIt,
      phoneNumber,
      email,
      property,
      communityCode,
      serviceIssue,
      serviceRequest,
      fullAddress,
      pickUpDateTime,
      byWhen,
      keysNeeded,
      rfid: inputRfid,
      status,
      lostReason,
      types,
      unit,
    } = req.body;

    const vendorName = (vendor || whoHasIt || "").trim();
    const propertyName = (property || communityCode || "").trim();
    const serviceIssueNum = (serviceIssue || serviceRequest || "").trim();

    if (!vendorName || !propertyName) {
      return res.status(400).json({
        message: "Mandatory fields: Vendor and Property are required.",
      });
    }

    let rfid = inputRfid ? inputRfid.trim() : "";
    if (!rfid) {
      rfid = `REQ-${Date.now()}-${Math.floor(1000 + Math.random() * 9000)}`;
    }

    // Check if RFID already exists in DB to prevent unique constraint failure
    const existingKey = await Key.findOne({ rfid });
    if (existingKey) {
      rfid = `${rfid}-${Date.now()}`;
    }

    const keyStatus = status || "Key Requested";
    let logMessage = `${new Date().toLocaleString()}: Key request created. Vendor: ${vendorName}, Property: ${propertyName}`;
    if (keyStatus === "Lost" && lostReason) {
      logMessage += `. Reason: ${lostReason}`;
    }

    const newKey = new Key({
      rfid,
      communityCode: propertyName,
      unit: unit || "",
      types:
        types && types.length ? types : keysNeeded ? [keysNeeded] : ["Unit"],
      whoHasIt: vendorName,
      serviceRequest: serviceIssueNum,
      serviceStatus: "",
      status: keyStatus,
      lostReason: lostReason || "",
      source: "System",
      isReturned: keyStatus === "Checked In",
      returnedAt: keyStatus === "Checked In" ? new Date() : null,
      vendor: vendorName,
      phoneNumber: phoneNumber || "",
      email: email || "",
      property: propertyName,
      serviceIssue: serviceIssueNum,
      fullAddress: fullAddress || "",
      pickUpDateTime: pickUpDateTime || "",
      byWhen: byWhen || "",
      keysNeeded: keysNeeded || "",
      accessLog: [logMessage],
    });

    await newKey.save();
    sendKeyStatusEmail(newKey);
    return res.json({
      message: "Key request created successfully",
      key: newKey,
    });
  } catch (error) {
    next(error);
  }
};
