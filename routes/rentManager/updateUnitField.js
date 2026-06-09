import axios from "axios";
import { logRMAction } from "../../utils/rentManager.js";

function getAnswerByName(answers, fieldName, keyName = "answer") {
  if (!answers) return null;
  const field = Object.values(answers).find((item) => item.text === fieldName);
  if (!field) return null;

  const value = field?.[keyName] || null;

  if (typeof value === "string") {
    return value.replace(/<[^>]*>/g, "").trim();
  }

  return value;
}

export default async function updateUnitField({
  cfg,
  answers,
  unitId,
  headers,
  action,
  valueToUse,
  itemType,
  pdfLink,
  udfId,
  belongsTo,
  email,
  submissionID,
}) {
  try {
    if (belongsTo !== "udf") {
      const errorMsg = "You can only update UDFs for Unit table";
      logRMAction({
        type: "WEBHOOK_UNIT_UDF_FAILURE",
        email,
        formId: cfg?.formId,
        submissionID,
        error: errorMsg,
      });
      return {
        status: "error",
        error: errorMsg,
      };
    }

    let finalValue = "";
    if (action === "replace") {
      finalValue = valueToUse;

      if (itemType === "pdf") {
        finalValue = pdfLink;
      }

      if (itemType === "jotform") {
        const jotformValue = getAnswerByName(answers, valueToUse);
        finalValue = jotformValue || "";
      }
    } else if (action === "prepend") {
      const detailRes = await axios.get(
        `${process.env.RM_BASE_URL}/Units/${unitId}`,
        {
          headers,
          params: { embeds: "UserDefinedValues" },
        }
      );
      const currentUdfObj = detailRes.data.UserDefinedValues?.find(
        (v) => v.UserDefinedFieldID === udfId
      );
      const currentValue = currentUdfObj ? currentUdfObj.Value || "" : "";
      finalValue = valueToUse + currentValue;

      if (itemType === "pdf") {
        const cleanCurrent = currentValue?.trim() || "";
        finalValue = cleanCurrent ? `${pdfLink} | ${cleanCurrent}` : pdfLink;
      }

      if (itemType === "jotform") {
        const jotformValue = getAnswerByName(answers, valueToUse);
        finalValue = jotformValue + currentValue;
      }
    } else if (action === "empty") {
      finalValue = "";
    } else {
      const errorMsg = `Unknown action: ${action}`;
      logRMAction({
        type: "WEBHOOK_UNIT_UDF_FAILURE",
        email,
        formId: cfg?.formId,
        submissionID,
        error: errorMsg,
      });
      return {
        status: "skipped",
        reason: errorMsg,
      };
    }

    await axios.post(
      `${process.env.RM_BASE_URL}/Units/UserDefinedValues`,
      [
        {
          ParentID: unitId,
          UserDefinedFieldID: udfId,
          Value: finalValue,
        },
      ],
      { headers }
    );

    return {
      status: "success",
      value: finalValue,
    };
  } catch (err) {
    logRMAction({
      type: "WEBHOOK_UNIT_UDF_FAILURE",
      email,
      formId: cfg?.formId,
      submissionID,
      error: err.message,
    });
    return {
      status: "error",
      error: err.message,
    };
  }
}
