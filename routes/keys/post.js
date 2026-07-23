import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

export default async (req, res, next) => {
  try {
    const {
      rfid,
      communityCode,
      unit,
      types,
      whoHasIt,
      serviceRequest,
      status,
      lostReason,
      serviceStatus,
    } = req.body;

    if (!rfid || !communityCode || !whoHasIt || !types || !types.length) {
      return res.status(400).json({
        message:
          "Mandatory fields: RFID, Community Code, Requester, and Key Types are required.",
      });
    }

    // Check if key is already checked out
    const existingKey = await Key.findOne({ rfid });

    const keyStatus = status || "Checked Out";
    let logMessage = `${new Date().toLocaleString()}: Logged status "${keyStatus}" for ${whoHasIt}`;
    if (keyStatus === "Lost" && lostReason) {
      logMessage += `. Reason: ${lostReason}`;
    }

    if (existingKey) {
      if (!existingKey.isReturned && existingKey.status !== "Checked In") {
        return res.status(400).json({
          message: `Key with RFID ${rfid} is already registered under status "${existingKey.status}" to ${existingKey.whoHasIt}.`,
        });
      }

      existingKey.communityCode = communityCode;
      existingKey.unit = unit || "";
      existingKey.types = types;
      existingKey.whoHasIt = whoHasIt;
      existingKey.serviceRequest = serviceRequest || "";
      existingKey.serviceStatus = serviceStatus || "";
      existingKey.status = keyStatus;
      existingKey.lostReason = lostReason || "";
      existingKey.isReturned = keyStatus === "Checked In";
      existingKey.returnedAt = keyStatus === "Checked In" ? new Date() : null;
      existingKey.source = "System"; //to track the key where it create, from system or webhook
      existingKey.accessLog.push(logMessage);

      await existingKey.save();
      sendKeyStatusEmail(existingKey); // send email in background
      return res.json({
        message: "Key logged successfully",
        key: existingKey,
      });
    }

    const newKey = new Key({
      rfid,
      communityCode,
      unit: unit || "",
      types,
      whoHasIt,
      serviceRequest: serviceRequest || "",
      serviceStatus: serviceStatus || "",
      status: keyStatus,
      lostReason: lostReason || "",
      source: "System",
      isReturned: keyStatus === "Checked In",
      returnedAt: keyStatus === "Checked In" ? new Date() : null,
      accessLog: [logMessage],
    });

    await newKey.save();
    sendKeyStatusEmail(newKey); // send email in background
    return res.json({ message: "Key checked out successfully", key: newKey });
  } catch (error) {
    next(error);
  }
};
