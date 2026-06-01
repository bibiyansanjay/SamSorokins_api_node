import { Router } from "express";
import axios from "axios";
import multer from "multer";
const upload = multer();

import {
  getRMHeaders,
  getMatchingRows,
  getJotformSubmission,
  resolveJotformAnswer,
  logRMAction,
} from "../../utils/rentManager";
import updateSystemField from "./updateSystemField.js";
// import test from "./test";

const router = Router();

const apiKey =
  process.env.JOTFORM_API_KEY || "da724b69ac2c6dc23adf791b768e8674";

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

/**
 * GET /health
 * Simple liveness check endpoint.
 */
router.get("/health", (req, res) => {
  res.status(200).json({
    status: "ok",
    timestamp: new Date().toISOString(),
    rmBaseUrl: process.env.RM_BASE_URL,
  });
});

/**
 * POST /webhook-udf
 * Specialized webhook for dynamic UDF updates (Replace, Prepend, Empty).
 */
router.post("/webhook-udf", upload.none(), async (req, res) => {
  //   console.log("req.body", req.body);

  // const { email, formId, submissionID } = req.body;
  // Jotform sometimes sends data in a 'rawRequest' string
  let payload = req.body;
  if (req.body.rawRequest) {
    try {
      const raw = JSON.parse(req.body.rawRequest);
      payload = { ...payload, ...raw };
    } catch (e) {
      console.warn("[Webhook-UDF] Failed to parse rawRequest:", e.message);
    }
  }

  // 1. Precise extraction with common Jotform variants
  const formId =
    payload.formId || payload.formID || payload.q4_formId || payload.q1_formID;
  const submissionID =
    payload.submissionID ||
    payload.submissionId ||
    payload.q5_submissionId ||
    payload.q2_submissionID;

  let jotformData = null;
  let answers = {};
  let jotFormEmail = null;

  if (submissionID) {
    try {
      // Get submission data from jot form
      const response = await axios.get(
        `https://premiumpd.jotform.com/API/submission/${submissionID}`,
        {
          params: { apiKey },
        }
      );

      jotformData = response?.data;
      answers = jotformData?.content?.answers || {};
      // jotFormEmail = getAnswerByName(answers, "User Email");
      jotFormEmail = getAnswerByName(answers, "Tenant Status Email");

      console.log(
        "Received data from jot form",
        "form id:",
        jotformData?.content?.form_id,
        "jotFormEmail:",
        jotFormEmail,
        "submissionId:",
        submissionID
      );
    } catch (error) {
      console.warn(
        `[Webhook-UDF] Failed to fetch Jotform submission ${submissionID}:`,
        error.message
      );
    }
  }

  // 2. Email extraction - search for 'email' in any key if top-level variants fail
  // let email =
  //   payload.email ||
  //   payload.userEmail ||
  //   payload.q415_userEmail ||
  //   payload.q3_email ||
  //   jotFormEmail;

  const email = jotFormEmail;
  console.log({ email }, { submissionID });
  //Tenant Status Email

  if (!email) {
    // Robust search: find any key that contains "email" (case-insensitive) and has a value that looks like an email
    const emailKey = Object.keys(payload).find(
      (key) =>
        key.toLowerCase().includes("email") &&
        typeof payload[key] === "string" &&
        payload[key].includes("@")
    );
    if (emailKey) email = payload[emailKey];
  }

  if (!email || !formId) {
    logRMAction({
      type: "WEBHOOK_CONFIG_NOT_FOUND",
      email,
      formId,
      submissionID,
      error: `email and formId are required ${formId}`,
    });

    return res.status(400).json({ error: "email and formId are required" });
  }

  console.log(
    `[Webhook-UDF] Extracted -> Email: ${email}, FormId: ${formId}, SubmissionID: ${submissionID}`
  );
  console.log(
    `\n>>> Dynamic UDF Webhook received. Email: ${email}, FormId: ${formId}`
  );

  try {
    const headers = await getRMHeaders();
    const configs = await getMatchingRows(formId);
    if (configs?.length === 0) {
      logRMAction({
        type: "WEBHOOK_CONFIG_NOT_FOUND",
        email,
        formId,
        submissionID,
        error: `No active config found in Google Sheets for form ${formId}`,
      });
      return res
        .status(404)
        .json({ error: `No active config for form ${formId}` });
    }

    // let answers = {};
    // if (submissionID) {
    //   try {
    //     const submission = await getJotformSubmission(submissionID);
    //     answers = submission.answers;
    //   } catch (e) {
    //     console.warn(
    //       `[Webhook-UDF] Could not fetch submission ${submissionID}: ${e.message}`
    //     );
    //   }
    // }

    const searchRes = await axios.get(
      `${process.env.RM_BASE_URL}/Tenants/Search`,
      {
        headers,
        params: {
          filterExpression: `Contacts.Email,eq,${email}`,
          fields: "TenantID",
          // pageSize: 1,
          pageSize: 5, // allow up to 5 to detect duplicates, we'll handle the error if more than 1 is found
        },
      }
    );

    const tenants = searchRes?.data;
    if (!Array.isArray(tenants) || tenants.length === 0) {
      console.error("tenant not found", email);

      logRMAction({
        type: "WEBHOOK_TENANT_NOT_FOUND",
        email,
        formId,
        submissionID,
        error: `Tenant not found in Rent Manager with email ${email}`,
      });
      return res
        .status(404)
        .json({ error: `Tenant not found with email ${email}` });
    }

    if (Array.isArray(tenants) && tenants?.length > 1) {
      console.error("getting multiple tenants with this email", email);

      logRMAction({
        type: "WEBHOOK_MULTIPLE_TENANTS_FOUND",
        email,
        formId,
        submissionID,
        error: "Getting multiple tenants with this email",
        tenantCount: tenants?.length,
        tenantIds: tenants?.map((t) => t.TenantID),
      });

      return res.status(400).json({
        success: false,
        error: "Getting multiple tenants with this email",
      });
    }

    const tenantId = tenants[0]?.TenantID;

    const results = [];
    for (const cfg of configs) {
      try {
        let valueToUse = cfg.item;
        // if (submissionID && cfg.item?.startsWith("q")) {
        //   valueToUse = resolveJotformAnswer(answers, cfg.item);
        // }
        const belongsTo = cfg?.belongsTo?.toLowerCase()?.trim() || ""; // fields belong to UDF or System field

        const udfRes = await axios.get(
          `${process.env.RM_BASE_URL}/UserDefinedFields`,
          {
            headers,
            params: { filters: `Name,eq,${cfg.field}` },
          }
        );

        const fields = udfRes?.data;
        if (
          (!Array.isArray(fields) || fields.length === 0) &&
          belongsTo !== "system field"
        ) {
          logRMAction({
            type: "WEBHOOK_UDF_FIELD_NOT_FOUND",
            email,
            formId,
            field: cfg.field,
            error: `UDF field "${cfg.field}" not found in Rent Manager`,
          });
          results.push({
            field: cfg.field,
            status: "error",
            error: "Field not found",
          });
          continue;
        }
        const udfId = fields?.[0]?.UserDefinedFieldID;

        let finalValue = "";
        const action = cfg.action?.toLowerCase()?.trim();
        // const itemValue = cfg?.item?.toLowerCase(); // item name from sheet
        const reportID = cfg?.extraInfo?.trim() || ""; // report ID from extraInfo column
        const itemType = cfg?.itemType?.toLowerCase()?.trim() || ""; // item type from sheet (e.g. "PDF")

        const tableName = cfg?.tableName?.toLowerCase()?.trim() || ""; // table name in Rent Manager

        const pdfLink = `https://premiumpd.jotform.com/API/generatePDF?formid=${formId}&submissionid=${submissionID}&download=1&reportid=${reportID}&apiKey=${apiKey}`;
        console.log(submissionID, "PDF Link for submission:", pdfLink);

        if (tableName !== "tenant") {
          // return error if table name is not tenant, since that's the only one we support in this webhook for now
          const errorMsg = `Unsupported table name: ${tableName} for ${cfg.field}. Only "tenant" is supported in this webhook.`;
          logRMAction({
            type: "WEBHOOK_UDF_FAILURE",
            email,
            formId,
            submissionID,
            error: errorMsg,
          });
          console.error("[Webhook-UDF] Error:", errorMsg);
          return res.status(500).json({ success: false, error: errorMsg });
        }

        if (belongsTo === "system field") {
          console.log("Processing system field:", cfg.field);
          const result = await updateSystemField({
            cfg,
            answers,
            tenantId,
            headers,
            action,
          });
          results.push({
            field: cfg.field,
            ...result,
          });
          continue;
        } else {
          if (action === "replace") {
            finalValue = valueToUse;

            if (itemType === "pdf") {
              finalValue = pdfLink;
            }

            if (itemType === "jotform") {
              //If item type is jotform, get field value from jotform answers and update field value in rent manager with that value
              const jotformValue = getAnswerByName(answers, valueToUse);
              finalValue = jotformValue || "";
            }
          } else if (action === "prepend") {
            const detailRes = await axios.get(
              `${process.env.RM_BASE_URL}/Tenants/${tenantId}`,
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

            // prepend PDF link if item type is PDF
            if (itemType === "pdf") {
              const cleanCurrent = currentValue?.trim() || "";

              finalValue = cleanCurrent
                ? `${pdfLink} | ${cleanCurrent}`
                : pdfLink;
            }

            if (itemType === "jotform") {
              //If item type is jotform, get field value from jotform answers and update field value in rent manager with that value
              const jotformValue = getAnswerByName(answers, valueToUse);
              finalValue = jotformValue + currentValue;
            }
          } else if (action === "empty") {
            finalValue = "";
          } else {
            results.push({
              field: cfg.field,
              status: "skipped",
              reason: `Unknown action: ${action}`,
            });
            continue;
          }

          await axios.post(
            `${process.env.RM_BASE_URL}/Tenants/UserDefinedValues`,
            [
              {
                ParentID: tenantId,
                UserDefinedFieldID: udfId,
                Value: finalValue,
              },
            ],
            { headers }
          );

          results.push({
            field: cfg.field,
            action,
            status: "success",
            value: finalValue,
          });
        }
      } catch (err) {
        results.push({ field: cfg.field, status: "error", error: err.message });
      }
    }

    // logRMAction({
    //   type: "WEBHOOK_UDF",
    //   email,
    //   formId,
    //   submissionID,
    //   tenantId,
    //   configs, // matched rows
    //   results, // actions performed with status and reason
    // });

    // const hasError = results.some((r) => r.status === "error" || r.error);
    // if (hasError) {
    //   logRMAction({
    //     type: "WEBHOOK_UDF_PARTIAL_FAILURE",
    //     email,
    //     formId,
    //     submissionID,
    //     tenantId,
    //     results,
    //   });
    // }

    return res.status(200).json({ success: true, tenantId, results });
  } catch (error) {
    logRMAction({
      type: "WEBHOOK_UDF_FAILURE",
      email,
      formId,
      submissionID,
      error: error.message,
    });
    console.error("[Webhook-UDF] Error:", error.message);
    return res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * GET /tenants/email/:email
 */
router.get("/tenants/email/:email", async (req, res) => {
  const email = req.params.email;
  if (!email)
    return res.status(400).json({ error: "Email parameter is required" });

  try {
    const headers = await getRMHeaders();
    const response = await axios.get(
      `${process.env.RM_BASE_URL}/Tenants/Search`,
      {
        headers,
        params: {
          filterExpression: `Contacts.Email,eq,${email}`,
          embeds: "Contacts,Contacts.PhoneNumbers,UserDefinedValues",
          pageSize: 1,
        },
      }
    );

    const tenants = response.data;
    if (!Array.isArray(tenants) || tenants.length === 0) {
      logRMAction({
        type: "GET_TENANT_BY_EMAIL_NOT_FOUND",
        email,
        error: `No tenant found with email: ${email}`,
      });
      return res.status(404).json({
        success: false,
        error: `No tenant found with email: ${email}`,
      });
    }

    return res.status(200).json({ success: true, tenant: tenants[0] });
  } catch (error) {
    return res
      .status(error.response?.status || 500)
      .json({ success: false, error: error.response?.data || error.message });
  }
});

/**
 * GET /tenants/email/:email/udf
 */
router.get("/tenants/email/:email/udf", async (req, res) => {
  const email = req.params.email;
  const udfName = req.query.name || "Turnover Move In Condition URL";
  const udfValue = req.query.value || "";

  if (!email) return res.status(400).json({ error: "Email is required" });

  try {
    const headers = await getRMHeaders();
    const searchRes = await axios.get(
      `${process.env.RM_BASE_URL}/Tenants/Search`,
      {
        headers,
        params: {
          filterExpression: `Contacts.Email,eq,${email}`,
          fields: "TenantID",
          pageSize: 1,
        },
      }
    );

    const tenants = searchRes.data;
    if (!Array.isArray(tenants) || tenants.length === 0) {
      logRMAction({
        type: "DIRECT_UDF_TENANT_NOT_FOUND",
        email,
        error: `Tenant not found with email ${email}`,
      });
      return res
        .status(404)
        .json({ error: `Tenant not found with email ${email}` });
    }
    const tenantId = tenants[0].TenantID;

    const udfRes = await axios.get(
      `${process.env.RM_BASE_URL}/UserDefinedFields`,
      {
        headers,
        params: { filters: `Name,eq,${udfName}` },
      }
    );

    const fields = udfRes.data;
    if (!Array.isArray(fields) || fields.length === 0) {
      logRMAction({
        type: "DIRECT_UDF_FIELD_NOT_FOUND",
        email,
        udfName,
        error: `UDF field not found: ${udfName}`,
      });
      return res.status(404).json({ error: `UDF field not found: ${udfName}` });
    }
    const udfId = fields[0].UserDefinedFieldID;

    const updateRes = await axios.post(
      `${process.env.RM_BASE_URL}/Tenants/UserDefinedValues`,
      [{ ParentID: tenantId, UserDefinedFieldID: udfId, Value: udfValue }],
      { headers }
    );

    logRMAction({
      type: "DIRECT_UDF_UPDATE",
      email,
      udfName,
      udfValue,
      tenantId,
      status: "success",
      message: `Updated "${udfName}"`,
    });

    return res.status(200).json({
      success: true,
      message: `Updated "${udfName}" for Tenant ID ${tenantId}`,
      details: { tenantId, udfId, udfName, udfValue },
      data: updateRes.data,
    });
  } catch (error) {
    logRMAction({
      type: "DIRECT_UDF_UPDATE_FAILURE",
      email,
      udfName,
      udfValue,
      error: error.message,
    });
    return res
      .status(error.response?.status || 500)
      .json({ success: false, error: error.response?.data || error.message });
  }
});

/**
 * GET /tenants
 */
router.get("/tenants", async (req, res) => {
  try {
    const pageSize = parseInt(req.query.pageSize, 10) || 50;
    const pageNumber = parseInt(req.query.pageNumber, 10) || 1;
    const search = req.query.search || null;

    const headers = await getRMHeaders();
    const params = {
      pageSize,
      pageNumber,
      fields: "TenantID,FirstName,LastName,Status",
    };
    if (search) params.filterExpression = `LastName,ct,${search}`;

    const response = await axios.get(
      `${process.env.RM_BASE_URL}/Tenants/Search`,
      {
        headers,
        params: { ...params, embeds: "Contacts,Contacts.PhoneNumbers" },
      }
    );

    const tenants = response.data;
    return res.status(200).json({
      success: true,
      count: tenants.length || 0,
      pageSize,
      pageNumber,
      tenants,
    });
  } catch (error) {
    return res
      .status(error.response?.status || 500)
      .json({ success: false, error: error.response?.data || error.message });
  }
});

export default router;
