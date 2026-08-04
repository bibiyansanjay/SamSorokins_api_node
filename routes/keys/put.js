import { Key } from "../../models";
import { sendKeyStatusEmail } from "../../methods/sendKeyStatusEmail";
import { updateRentManagerKeyStatus } from "../../utils/rentManager";

export default async (req, res, next) => {
  try {
    const { id } = req.params;
    const { vendor, rfId, whoHasIt, status, lostReason } = req.body;

    const key = await Key.findById(id);
    if (!key) {
      return res.status(404).json({ message: "Key record not found." });
    }

    // let isChanged = false;
    let statusChanged = false;
    const newVendor = (vendor || whoHasIt || "").trim();

    if (newVendor && newVendor !== key.vendor) {
      const oldVendor = key.vendor || "Unknown";
      key.vendor = newVendor;
      key.accessLog.push(
        `${new Date().toLocaleString()}: Vendor updated from ${oldVendor} to ${newVendor}`
      );
      // isChanged = true;
    }

    if (lostReason !== undefined && lostReason !== key.lostReason) {
      key.lostReason = lostReason.trim();
      // isChanged = true;
    }

    if (status !== undefined && status !== key.status) {
      const oldStatus = key.status;
      key.status = status;

      if (status === "Checked In") {
        key.isReturned = true;
        key.returnedAt = new Date();
      } else {
        key.isReturned = false;
        key.returnedAt = null;
        key.isAccountingEmailSent = false;
      }

      let logText = `${new Date().toLocaleString()}: Status updated from "${oldStatus}" to "${status} RFID: ${
        rfId || "None"
      }"`;
      if (status === "Lost" && (lostReason || key.lostReason)) {
        logText += `. Reason: ${lostReason || key.lostReason}`;
      }
      key.accessLog.push(logText);

      statusChanged = true;
    }

    if (rfId !== undefined) {
      const trimmedRfId = (rfId || "").trim();
      if (trimmedRfId !== key.rfId) {
        // const oldRfId = key.rfId || "None";
        key.rfId = trimmedRfId;
        // key.accessLog.push(
        //   `${new Date().toLocaleString()}: RFID / Key ID updated from "${oldRfId}" to "${trimmedRfId}"`
        // );
      }
    }

    await key.save();
    if (statusChanged) {
      sendKeyStatusEmail(key); // send email in background
      if (["Checked Out", "Checked In", "Lost"].includes(key.status)) {
        updateRentManagerKeyStatus(key).catch((err) =>
          console.error("[RM UpdateKeyStatus Error]", err)
        );
      }
    }

    return res.json({ message: "Key updated successfully", key });
  } catch (error) {
    next(error);
  }
};
