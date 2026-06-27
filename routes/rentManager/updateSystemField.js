import axios from "axios";
import { logRMAction } from "../../utils/rentManager.js";

const stateMap = {
  alabama: "AL",
  alaska: "AK",
  arizona: "AZ",
  arkansas: "AR",
  california: "CA",
  colorado: "CO",
  connecticut: "CT",
  delaware: "DE",
  florida: "FL",
  georgia: "GA",
  hawaii: "HI",
  idaho: "ID",
  illinois: "IL",
  indiana: "IN",
  iowa: "IA",
  kansas: "KS",
  kentucky: "KY",
  louisiana: "LA",
  maine: "ME",
  maryland: "MD",
  massachusetts: "MA",
  michigan: "MI",
  minnesota: "MN",
  mississippi: "MS",
  missouri: "MO",
  montana: "MT",
  nebraska: "NE",
  nevada: "NV",
  "new hampshire": "NH",
  "new jersey": "NJ",
  "new mexico": "NM",
  "new york": "NY",
  "north carolina": "NC",
  "north dakota": "ND",
  ohio: "OH",
  oklahoma: "OK",
  oregon: "OR",
  pennsylvania: "PA",
  "rhode island": "RI",
  "south carolina": "SC",
  "south dakota": "SD",
  tennessee: "TN",
  texas: "TX",
  utah: "UT",
  vermont: "VT",
  virginia: "VA",
  washington: "WA",
  "west virginia": "WV",
  wisconsin: "WI",
  wyoming: "WY",
};

function normalizeDateToYYYYMMDD(dateStr) {
  if (!dateStr) return null;
  const s = String(dateStr).trim();

  // Case 1: YYYY-MM-DD (e.g. 2026-06-17)
  const ymdMatch = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
  if (ymdMatch) {
    const [_, year, month, day] = ymdMatch;
    return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  }

  // Case 2: MM/DD/YYYY or MM-DD-YYYY (e.g. 06/16/2026, 6-18-2026)
  const mdyMatch = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})/);
  if (mdyMatch) {
    const [_, month, day, year] = mdyMatch;
    return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  }

  // Fallback to JS Date parsing
  const d = new Date(s);
  if (!isNaN(d.getTime())) {
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }

  return s; // If all parsing fails, return as-is
}

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

export default async function updateSystemField({
  cfg,
  answers,
  tenantId,
  headers,
  action,
  email,
  submissionID,
}) {
  try {
    const logAndReturnError = (errorMsg) => {
      if (submissionID) {
        logRMAction({
          type: "WEBHOOK_SYSTEM_FIELD_FAILURE",
          email,
          formId: cfg?.formId,
          submissionID,
          error: errorMsg,
        });
      }
      return {
        status: "error",
        error: errorMsg,
      };
    };
    const fieldName = cfg.field?.trim();
    const normalizedField = fieldName.toLowerCase();
    let updateField = null;

    console.log("Field to update in system field: ", normalizedField);
    if (
      normalizedField === "moveoutdate" ||
      normalizedField === "move out date" ||
      normalizedField === "move out"
    ) {
      updateField = "MoveOutDate";
    } else if (
      normalizedField === "noticedate" ||
      normalizedField === "notice date" ||
      normalizedField === "notice given date" ||
      normalizedField === "notice given" ||
      normalizedField === "notice"
    ) {
      updateField = "NoticeDate";
    } else if (
      normalizedField === "lease end date" ||
      normalizedField === "lease end" ||
      normalizedField === "leaseenddate"
    ) {
      updateField = "LeaseEndDate";
    }

    if (updateField) {
      let finalValue = null;
      if (action !== "empty") {
        let answerObj = null;
        if (cfg.item) {
          answerObj = getAnswerByName(answers, cfg.item, "answer");
        }
        if (!answerObj) {
          answerObj = getAnswerByName(answers, fieldName, "answer");
        }
        console.log(updateField, " +", cfg.item, "+", answerObj);
        if (answerObj) {
          if (typeof answerObj === "object") {
            const { year, month, day } = answerObj;
            if (year && month && day) {
              finalValue = `${year}-${String(month).padStart(2, "0")}-${String(
                day
              ).padStart(2, "0")}`;
            } else {
              return logAndReturnError(
                `Invalid date object format in Jotform for field: ${fieldName}`
              );
            }
          } else {
            const rawVal = String(answerObj).trim();
            if (!rawVal) {
              return logAndReturnError(
                `Empty date string in Jotform for field: ${fieldName}`
              );
            }
            finalValue = normalizeDateToYYYYMMDD(rawVal);
          }
        } else {
          // If no answer found, check if cfg.item has a static date
          if (!cfg.item || cfg.item.startsWith("q")) {
            return logAndReturnError(
              `No date answer found in Jotform for field: ${fieldName}`
            );
          }
          finalValue = normalizeDateToYYYYMMDD(cfg.item);
        }
      }

      // Check if it is a Lease field
      let leaseFieldName = null;
      if (updateField === "MoveOutDate") {
        leaseFieldName = "MoveOutDate";
      } else if (updateField === "LeaseEndDate") {
        leaseFieldName = "EndDate";
      } else if (updateField === "NoticeDate") {
        leaseFieldName = "NoticeDate";
      }

      if (leaseFieldName) {
        const tenantRes = await axios.get(
          `${process.env.RM_BASE_URL}/Tenants/${tenantId}`,
          {
            headers,
            params: { embeds: "Leases" },
          }
        );
        const tenant = tenantRes.data;
        const lease =
          tenant?.Leases?.find((l) => l.IsPrimaryLease) || tenant?.Leases?.[0];
        if (!lease) {
          return logAndReturnError(
            `No leases found for tenant ID ${tenantId} to update Lease field: ${leaseFieldName}`
          );
        }

        console.log(
          `[System Field] Fetching full Lease details for LeaseID: ${lease.LeaseID}...`
        );
        const leaseRes = await axios.get(
          `${process.env.RM_BASE_URL}/Leases/${lease.LeaseID}`,
          { headers }
        );
        const fullLease = leaseRes.data;
        fullLease[leaseFieldName] = finalValue;

        console.log(
          `[System Field] Updating Lease ${lease.LeaseID} (Tenant ${tenantId}) field ${leaseFieldName} to:`,
          finalValue
        );
        await axios.post(`${process.env.RM_BASE_URL}/Leases`, [fullLease], {
          headers,
        });

        return {
          status: "success",
          value: finalValue,
        };
      }

      console.log(
        `[System Field] Fetching full Tenant details for TenantID: ${tenantId}...`
      );
      const tenantRes = await axios.get(
        `${process.env.RM_BASE_URL}/Tenants/${tenantId}`,
        { headers }
      );
      const tenant = tenantRes.data;
      tenant[updateField] = finalValue;

      console.log(
        `[System Field] Updating Tenant ${tenantId} field ${updateField} to:`,
        finalValue
      );

      await axios.post(`${process.env.RM_BASE_URL}/Tenants`, [tenant], {
        headers,
      });

      return {
        status: "success",
        value: finalValue,
      };
    }

    let addressTypeId = null;

    if (fieldName === "Forwarding Address") {
      addressTypeId = 15;
    } else if (fieldName === "Primary Address") {
      addressTypeId = 1;
    } else if (fieldName === "Alternate Address") {
      addressTypeId = 2;
    }

    if (!addressTypeId) {
      return logAndReturnError(`Unsupported system field: ${fieldName}`);
    }

    const addrObj = getAnswerByName(answers, fieldName, "answer");

    console.log("Address received from Jotform:", addrObj);

    if (!addrObj || typeof addrObj !== "object") {
      return logAndReturnError(
        `No valid address object found in Jotform answers for: ${fieldName}`
      );
    }

    const line1 = [addrObj?.addr_line1, addrObj?.addr_line2]
      .filter(Boolean)
      .join(" ");
    const stateLower = (addrObj?.state || "").toLowerCase().trim();
    const stateAbbr = stateMap[stateLower] || addrObj?.state || "";
    const formattedAddress = `${line1}\r\n${
      addrObj?.city || ""
    }, ${stateAbbr} ${addrObj?.postal || ""}`.trim();

    // const addressTypeRes = await axios.get(
    //   `${process.env.RM_BASE_URL}/Tenants/${tenantId}/Addresses`,
    //   {
    //     headers,
    //     params: {
    //       filters: `AddressTypeID,eq,${addressTypeId}`,
    //       fields: "AddressID",
    //     },
    //   }
    // );

    // const existingAddresses = addressTypeRes.data;
    // let addressId = null;
    // if (Array.isArray(existingAddresses) && existingAddresses.length > 0) {
    //   addressId = existingAddresses[0].AddressID;
    // }

    //2935 Telegraph Ave\r\nBerkeley, CA 94705
    const addressPayload = {
      AddressTypeID: addressTypeId,
      Address: action === "empty" ? "" : formattedAddress,
    };

    console.log("Address payload:", addressPayload);
    console.log("Tenant ID:", tenantId);

    // if (addressId) {
    //   addressPayload.AddressID = addressId;
    // }

    await axios.post(
      `${process.env.RM_BASE_URL}/Tenants/${tenantId}/Addresses`,
      [addressPayload],
      { headers }
    );

    return {
      status: "success",
      value: formattedAddress,
      //   addressId: addressId || "new",
    };
  } catch (err) {
    if (submissionID) {
      logRMAction({
        type: "WEBHOOK_SYSTEM_FIELD_FAILURE",
        email,
        formId: cfg?.formId,
        submissionID,
        error: err.message,
      });
    }
    return {
      status: "error",
      error: err.message,
    };
  }
}
