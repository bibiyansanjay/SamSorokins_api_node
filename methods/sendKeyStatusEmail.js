import sendMail from "./sendMail";

export const sendKeyStatusEmail = async (key) => {
  try {
    const subject = `[Key Vault Alert] Request for ${
      key.vendor || key.property || "Key"
    } is now: ${key.status}`;
    const recipient = "testrohit1993@gmail.com";
    const templateName = "template-keyStatusChange";
    const payload = { key };

    await sendMail(subject, payload, recipient, templateName);
    console.log(
      `[Email] Status change notification sent successfully to ${recipient}`
    );
  } catch (error) {
    console.error(
      "[Email] Failed to send status change email notification:",
      error.message || error
    );
  }
};

export default sendKeyStatusEmail;
