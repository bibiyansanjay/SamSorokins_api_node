import { JotformSubmission, Upload } from "../../models/index.js";

// In-memory cache for raw computed unresolved duplicate groups
let unresolvedRawCache = null;
let unresolvedCacheTime = 0;
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes TTL

export const clearDuplicatesCache = () => {
  unresolvedRawCache = null;
  unresolvedCacheTime = 0;
};

const computeDuplicateGroupsFromDB = async (targetFormId = null) => {
  const matchStage = { isDuplicateResolved: { $ne: true } };
  if (targetFormId) {
    matchStage.formId = String(targetFormId).trim();
  }

  const pipeline = [
    { $match: matchStage },
    {
      $project: {
        submissionId: 1,
        formId: 1,
        formName: 1,
        propertyName: 1,
        unitName: 1,
        clientName: 1,
        clientEmail: 1,
        fullAddress: 1,
        isSubmited: 1,
        createdAt: 1,
        answersArray: {
          $objectToArray: { $ifNull: ["$answers", {}] },
        },
      },
    },
    {
      $project: {
        submissionId: 1,
        isSubmited: 1,
        createdAt: 1,
        formId: {
          $ifNull: [
            "$formId",
            {
              $let: {
                vars: {
                  firstMatch: {
                    $arrayElemAt: [
                      {
                        $filter: {
                          input: "$answersArray",
                          as: "item",
                          cond: {
                            $in: ["$$item.v.text", ["Form ID", "formId"]],
                          },
                        },
                      },
                      0,
                    ],
                  },
                },
                in: "$$firstMatch.v.answer",
              },
            },
          ],
        },
        formName: {
          $ifNull: [
            "$formName",
            {
              $let: {
                vars: {
                  firstMatch: {
                    $arrayElemAt: [
                      {
                        $filter: {
                          input: "$answersArray",
                          as: "item",
                          cond: {
                            $in: [
                              "$$item.v.text",
                              ["Uploader Header", "Form Name", "FormName"],
                            ],
                          },
                        },
                      },
                      0,
                    ],
                  },
                },
                in: "$$firstMatch.v.answer",
              },
            },
          ],
        },
        propertyName: {
          $ifNull: [
            "$propertyName",
            {
              $let: {
                vars: {
                  firstMatch: {
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
                in: "$$firstMatch.v.answer",
              },
            },
          ],
        },
        unitName: {
          $ifNull: [
            "$unitName",
            {
              $let: {
                vars: {
                  firstMatch: {
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
                in: "$$firstMatch.v.answer",
              },
            },
          ],
        },
        clientName: {
          $ifNull: [
            "$clientName",
            {
              $let: {
                vars: {
                  firstMatch: {
                    $arrayElemAt: [
                      {
                        $filter: {
                          input: "$answersArray",
                          as: "item",
                          cond: { $eq: ["$$item.v.text", "User Name"] },
                        },
                      },
                      0,
                    ],
                  },
                },
                in: "$$firstMatch.v.answer",
              },
            },
          ],
        },
        clientEmail: {
          $ifNull: [
            "$clientEmail",
            {
              $let: {
                vars: {
                  firstMatch: {
                    $arrayElemAt: [
                      {
                        $filter: {
                          input: "$answersArray",
                          as: "item",
                          cond: { $eq: ["$$item.v.text", "User Email"] },
                        },
                      },
                      0,
                    ],
                  },
                },
                in: "$$firstMatch.v.answer",
              },
            },
          ],
        },
        fullAddress: {
          $ifNull: [
            "$fullAddress",
            {
              $let: {
                vars: {
                  firstMatch: {
                    $arrayElemAt: [
                      {
                        $filter: {
                          input: "$answersArray",
                          as: "item",
                          cond: { $eq: ["$$item.v.text", "Full Address"] },
                        },
                      },
                      0,
                    ],
                  },
                },
                in: "$$firstMatch.v.answer",
              },
            },
          ],
        },
      },
    },
    {
      $match: {
        formId: { $exists: true, $ne: null, $nin: ["", "undefined", "null"] },
        propertyName: {
          $exists: true,
          $ne: null,
          $nin: ["", "N/A", "n/a", "undefined", "null", "-"],
        },
        unitName: {
          $exists: true,
          $ne: null,
          $nin: ["", "N/A", "n/a", "undefined", "null", "-"],
        },
      },
    },
    {
      $addFields: {
        groupKey: {
          $concat: [
            { $toLower: "$formId" },
            ":::",
            { $toLower: "$propertyName" },
            ":::",
            { $toLower: "$unitName" },
          ],
        },
      },
    },
    {
      $group: {
        _id: "$groupKey",
        formId: { $first: "$formId" },
        formName: { $first: "$formName" },
        propertyName: { $first: "$propertyName" },
        unitName: { $first: "$unitName" },
        count: { $sum: 1 },
        submissions: {
          $push: {
            _id: "$_id",
            submissionId: "$submissionId",
            createdAt: "$createdAt",
            isSubmited: "$isSubmited",
            clientName: "$clientName",
            clientEmail: "$clientEmail",
            clientAddress: "$fullAddress",
          },
        },
      },
    },
    { $match: { count: { $gte: 2 } } },
    { $sort: { count: -1 } },
  ];

  return await JotformSubmission.aggregate(pipeline);
};

export default async (req, res, next) => {
  try {
    const now = Date.now();
    const targetFormId = req.query.formId ? String(req.query.formId).trim() : null;
    const page = Math.max(0, parseInt(req.query.page || 0, 10));
    const pageSize = Math.max(
      1,
      parseInt(req.query.pageSize || req.query.limit || 8, 10)
    );

    // Compute or use cached raw unresolved duplicate groups
    let activeRawGroups;
    if (targetFormId) {
      activeRawGroups = await computeDuplicateGroupsFromDB(targetFormId);
    } else {
      if (!unresolvedRawCache || now - unresolvedCacheTime > CACHE_TTL_MS) {
        unresolvedRawCache = await computeDuplicateGroupsFromDB();
        unresolvedCacheTime = now;
      }
      activeRawGroups = unresolvedRawCache || [];
    }

    const totalGroups = activeRawGroups.length;
    const totalDuplicateSubmissions = activeRawGroups.reduce(
      (sum, g) => sum + (g.count || g.submissions?.length || 0),
      0
    );

    const start = page * pageSize;
    const paginatedRawGroups = activeRawGroups.slice(start, start + pageSize);

    if (paginatedRawGroups.length === 0) {
      return res.json({
        success: true,
        totalGroups,
        totalDuplicateSubmissions,
        page,
        pageSize,
        duplicateGroups: [],
      });
    }

    // Fetch file uploads ONLY for submission IDs in the active page slice
    const pageSubIds = [];
    paginatedRawGroups.forEach((g) => {
      g.submissions.forEach((sub) => {
        if (sub.submissionId) pageSubIds.push(sub.submissionId);
      });
    });

    const uploads = await Upload.find(
      { submissionId: { $in: pageSubIds } },
      { submissionId: 1, filename: 1, s3Url: 1, status: 1, createdAt: 1 }
    ).lean();

    const uploadsMap = {};
    uploads.forEach((f) => {
      if (!uploadsMap[f.submissionId]) {
        uploadsMap[f.submissionId] = [];
      }
      uploadsMap[f.submissionId].push({
        _id: f._id,
        filename: f.filename,
        s3Url: f.s3Url,
        status: f.status,
        createdAt: f.createdAt,
      });
    });

    const duplicateGroups = paginatedRawGroups.map((group) => {
      const formattedSubmissions = group.submissions.map((sub) => {
        const files = uploadsMap[sub.submissionId] || [];

        let overallStatus = "Pending";
        if (files.length === 0 || !sub?.isSubmited) {
          overallStatus = "Pending";
        } else {
          const allUploaded = files.every((f) => f.status === "Uploaded");
          const anyUploading = files.some((f) => f.status === "Uploading");
          const anyFailed = files.some((f) => f.status === "Failed");

          if (allUploaded) overallStatus = "Uploaded";
          else if (anyUploading) overallStatus = "Uploading";
          else if (anyFailed) overallStatus = "Failed";
          else overallStatus = "Pending";
        }

        return {
          _id: sub._id,
          submissionId: sub.submissionId,
          createdAt: sub.createdAt,
          status: overallStatus,
          clientName: sub.clientName || "-",
          clientEmail: sub.clientEmail || "-",
          propertyName: group.propertyName,
          unitName: group.unitName,
          clientAddress: sub.clientAddress || "-",
          formName: group.formName,
          files,
          totalFiles: files.length,
        };
      });

      return {
        formId: group.formId,
        formName: group.formName,
        propertyName: group.propertyName,
        unitName: group.unitName,
        count: formattedSubmissions.length,
        submissions: formattedSubmissions,
      };
    });

    return res.json({
      success: true,
      totalGroups,
      totalDuplicateSubmissions,
      page,
      pageSize,
      duplicateGroups,
    });
  } catch (error) {
    console.error("[submissionsData/getDuplicates]", error);
    next(error);
  }
};
