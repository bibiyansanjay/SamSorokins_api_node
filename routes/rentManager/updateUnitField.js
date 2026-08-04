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

function addOneDayToDate(dateAnswer) {
  if (!dateAnswer) return "";

  let year, month, day;

  if (typeof dateAnswer === "object") {
    year = parseInt(dateAnswer.year, 10);
    month = parseInt(dateAnswer.month, 10);
    day = parseInt(dateAnswer.day, 10);
  } else {
    const s = String(dateAnswer).trim();

    // Case 1: YYYY-MM-DD
    const ymdMatch = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
    if (ymdMatch) {
      year = parseInt(ymdMatch[1], 10);
      month = parseInt(ymdMatch[2], 10);
      day = parseInt(ymdMatch[3], 10);
    } else {
      // Case 2: MM/DD/YYYY or MM-DD-YYYY
      const mdyMatch = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})/);
      if (mdyMatch) {
        year = parseInt(mdyMatch[3], 10);
        month = parseInt(mdyMatch[1], 10);
        day = parseInt(mdyMatch[2], 10);
      } else {
        // Fallback to JS Date parsingg
        const d = new Date(s);
        if (!isNaN(d.getTime())) {
          year = d.getFullYear();
          month = d.getMonth() + 1;
          day = d.getDate();
        }
      }
    }
  }

  if (year && month && day) {
    const d = new Date(year, month - 1, day);
    d.setDate(d.getDate() + 1);
    const nextYear = d.getFullYear();
    const nextMonth = String(d.getMonth() + 1).padStart(2, "0");
    const nextDay = String(d.getDate()).padStart(2, "0");
    return `${nextYear}-${nextMonth}-${nextDay}`;
  }

  return "";
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
    if (cfg.field?.trim() === "For Rent Available") {
      const leaseExpAnswer = getAnswerByName(answers, "Lease Expiration Date");
      if (!leaseExpAnswer) {
        const errorMsg = "Lease Expiration Date answer not found in Jotform";
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
      finalValue = addOneDayToDate(leaseExpAnswer);
      if (!finalValue) {
        const errorMsg = `Failed to parse Lease Expiration Date: ${JSON.stringify(
          leaseExpAnswer
        )}`;
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
    } else if (action === "replace") {
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
