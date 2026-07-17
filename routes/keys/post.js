import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";

export default async (req, res, next) => {
  try {
    const { rfid, communityCode, unit, types, whoHasIt, serviceRequest, status } =
      req.body;

    if (!rfid || !communityCode || !whoHasIt || !types || !types.length) {
      return res
        .status(400)
        .json({
          message:
            "Mandatory fields: RFID, Community Code, Requester, and Key Types are required.",
        });
    }

    // Check if key is already checked out
    const existingKey = await Key.findOne({ rfid });

    const logMessage = `${new Date().toLocaleString()}: Checked out by ${whoHasIt}`;

    if (existingKey) {
      if (!existingKey.isReturned) {
        return res
          .status(400)
          .json({
            message: `Key with RFID ${rfid} is already checked out to ${existingKey.whoHasIt}.`,
          });
      }

      existingKey.communityCode = communityCode;
      existingKey.unit = unit || "";
      existingKey.types = types;
      existingKey.whoHasIt = whoHasIt;
      existingKey.serviceRequest = serviceRequest || "";
      existingKey.status = status || "Key Requested";
      existingKey.isReturned = false;
      existingKey.returnedAt = null;
      existingKey.accessLog.push(logMessage);

      await existingKey.save();
      sendKeyStatusEmail(existingKey); // send email in background
      return res.json({
        message: "Key checked out successfully",
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
      status: status || "Key Requested",
      isReturned: false,
      accessLog: [logMessage],
    });

    await newKey.save();
    sendKeyStatusEmail(newKey); // send email in background
    return res.json({ message: "Key checked out successfully", key: newKey });
  } catch (error) {
    next(error);
  }
};
