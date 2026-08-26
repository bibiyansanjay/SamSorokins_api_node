import { JotformSubmission } from "../../models/index.js";

// In-memory cache for fast response times (< 1ms)
let optionsCache = null;
let cacheTime = 0;
const CACHE_TTL_MS = 60 * 1000; // 60 seconds cache TTL

export const clearOptionsCache = () => {
  optionsCache = null;
  cacheTime = 0;
};

export default async (req, res, next) => {
  try {
    const { propertyName = "" } = req.query;
    const selectedPropClean = (propertyName || "").trim().toLowerCase();
    const now = Date.now();

    // Perform MongoDB aggregation if cache has expired or is null
    if (!optionsCache || now - cacheTime > CACHE_TTL_MS) {
      const uniquePairs = await JotformSubmission.aggregate([
        {
          $project: {
            answersArray: { $objectToArray: { $ifNull: ["$answers", {}] } },
          },
        },
        {
          $project: {
            propertyName: {
              $let: {
                vars: {
                  target: {
                    $arrayElemAt: [
                      {
                        $filter: {
                          input: "$answersArray",
                          as: "item",
                          cond: {
                            $in: [
                              "$$item.v.text",
                              ["RM Short Name", "Property", "Property Name"],
                            ],
                          },
                        },
                      },
                      0,
                    ],
                  },
                },
                in: "$$target.v.answer",
              },
            },
            unitName: {
              $let: {
                vars: {
                  target: {
                    $arrayElemAt: [
                      {
                        $filter: {
                          input: "$answersArray",
                          as: "item",
                          cond: {
                            $in: [
                              "$$item.v.text",
                              [
                                "RM Unit Name",
                                "Unit",
                                "Unit Name",
                                "Unit Number",
                              ],
                            ],
                          },
                        },
                      },
                      0,
                    ],
                  },
                },
                in: "$$target.v.answer",
              },
            },
          },
        },
        {
          $group: {
            _id: {
              propertyName: {
                $cond: {
                  if: { $eq: [{ $type: "$propertyName" }, "string"] },
                  then: { $trim: { input: "$propertyName" } },
                  else: null,
                },
              },
              unitName: {
                $cond: {
                  if: { $eq: [{ $type: "$unitName" }, "string"] },
                  then: { $trim: { input: "$unitName" } },
                  else: null,
                },
              },
            },
          },
        },
      ]);

      optionsCache = uniquePairs.map((p) => ({
        propertyName: p._id.propertyName || "",
        unitName: p._id.unitName || "",
      }));
      cacheTime = now;
    }

    const propertiesSet = new Set();
    const unitsSet = new Set();

    optionsCache.forEach((item) => {
      const prop = item.propertyName;
      const unit = item.unitName;

      if (prop) {
        propertiesSet.add(prop);
      }

      if (unit) {
        const isAllProps = !selectedPropClean || selectedPropClean === "all";

        if (isAllProps || prop.toLowerCase() === selectedPropClean) {
          unitsSet.add(unit);
        }
      }
    });

    const properties = Array.from(propertiesSet).sort((a, b) =>
      a.localeCompare(b, undefined, { sensitivity: "base" })
    );
    const units = Array.from(unitsSet).sort((a, b) =>
      a.localeCompare(b, undefined, { sensitivity: "base" })
    );

    return res.json({
      success: true,
      properties: ["All", ...properties],
      units: ["All", ...units],
    });
  } catch (error) {
    console.error("[submissionsData/getOptions]", error);
    next(error);
  }
};
