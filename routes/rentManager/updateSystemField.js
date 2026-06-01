import axios from "axios";

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
}) {
  try {
    const fieldName = cfg.field?.trim();
    let addressTypeId = null;

    if (fieldName === "Forwarding Address") {
      addressTypeId = 15;
    } else if (fieldName === "Primary Address") {
      addressTypeId = 1;
    } else if (fieldName === "Alternate Address") {
      addressTypeId = 2;
    }

    if (!addressTypeId) {
      return {
        status: "skipped",
        reason: `Unsupported system field or AddressTypeID: ${fieldName}`,
      };
    }

    const addrObj = getAnswerByName(answers, fieldName, "answer");

    console.log("Address received from Jotform:", addrObj);

    if (!addrObj || typeof addrObj !== "object") {
      return {
        status: "skipped",
        reason: `No valid address object found in Jotform answers for: ${fieldName}`,
      };
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
    return {
      status: "error",
      error: err.message,
    };
  }
}
