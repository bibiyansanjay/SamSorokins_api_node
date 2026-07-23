import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

export default async (req, res, next) => {
  try {
    const {
      vendor,
      phoneNumber,
      email,
      property,
      serviceIssue,
      fullAddress,
      pickUpDateTime,
      byWhen,
      keysNeeded,
      status,
      lostReason,
    } = req.body;

    const vendorName = (vendor || "").trim();
    const propertyName = (property || "").trim();
    const serviceIssueNum = (serviceIssue || "").trim();

    if (!vendorName || !propertyName) {
      return res.status(400).json({
        message: "Mandatory fields: Vendor and Property are required.",
      });
    }

    const keyStatus = status || "Key Requested";
    let logMessage = `${new Date().toLocaleString()}: Key request created. Vendor: ${vendorName}, Property: ${propertyName}`;
    if (keyStatus === "Lost" && lostReason) {
      logMessage += `. Reason: ${lostReason}`;
    }

    const newKey = new Key({
      vendor: vendorName,
      phoneNumber: phoneNumber || "",
      email: email || "",
      property: propertyName,
      serviceIssue: serviceIssueNum,
      fullAddress: fullAddress || "",
      pickUpDateTime: pickUpDateTime || "",
      byWhen: byWhen || "",
      keysNeeded: keysNeeded || "",
      status: keyStatus,
      lostReason: lostReason || "",
      source: "System",
      isReturned: keyStatus === "Checked In",
      returnedAt: keyStatus === "Checked In" ? new Date() : null,
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
