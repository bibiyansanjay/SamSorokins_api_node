import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

export default async (req, res, next) => {
  try {
    const {
      vendor,
      phoneNumber,
      email,
      repairsEmail,
      property,
      unit,
      serviceIssue,
      fullAddress,
      pickUpDateTime,
      byWhen,
      keysNeeded,
      status,
      lostReason,
      userType,
      purpose,
      purposeDescription,
      areKeysForYou,
      whoWillPickUp,
      pickerPhoneNumber,
      pickerEmail,
      willBeReturned,
      whyNotReturned,
    } = req.body;

    const vendorName = (vendor || "").trim();
    const propertyName = (property || "").trim();
    const unitName = (unit || "").trim();
    const serviceIssueNum = (serviceIssue || "").trim();

    if (!vendorName || !propertyName) {
      return res.status(400).json({
        message: "Mandatory fields: Name/Vendor and Property are required.",
      });
    }

    const keyStatus = status || "Requested";
    const reqType = userType || "Vendor";
    let logMessage = `${new Date().toLocaleString()}: Key request created (${reqType}). Requester: ${vendorName}, Property: ${propertyName}${
      unitName ? ` (Unit: ${unitName})` : ""
    }`;
    if (keyStatus === "Lost" && lostReason) {
      logMessage += `. Reason: ${lostReason}`;
    }

    const newKey = new Key({
      vendor: vendorName,
      phoneNumber: phoneNumber || "",
      email: email || "",
      repairsEmail: repairsEmail || "repairs@premiumpd.com",
      property: propertyName,
      unit: unitName,
      serviceIssue: serviceIssueNum,
      fullAddress: fullAddress || "",
      pickUpDateTime: pickUpDateTime || "",
      byWhen: byWhen || "",
      keysNeeded: keysNeeded || "",
      status: keyStatus,
      lostReason: lostReason || "",
      userType: reqType,
      purpose: purpose || "",
      purposeDescription: purposeDescription || "",
      areKeysForYou: areKeysForYou || "",
      whoWillPickUp: whoWillPickUp || "",
      pickerPhoneNumber: pickerPhoneNumber || "",
      pickerEmail: pickerEmail || "",
      willBeReturned: willBeReturned || "",
      whyNotReturned: whyNotReturned || "",
      source: "System",
      isReturned: keyStatus === "Checked In",
      returnedAt: keyStatus === "Checked In" ? new Date() : null,
      accessLog: [logMessage],
    });

    await newKey.save();
    // sendKeyStatusEmail(newKey);
    return res.json({
      message: "Key request created successfully",
      key: newKey,
    });
  } catch (error) {
    next(error);
  }
};
