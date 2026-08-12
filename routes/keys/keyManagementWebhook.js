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

export default async (req, res, next) => {
  try {
    let payload = req.body;

    console.log("[Keys-Webhook] Received payload:", payload);
  } catch (error) {
    console.error("[Keys-Webhook] Error processing:", error);
    next(error);
  }
};
