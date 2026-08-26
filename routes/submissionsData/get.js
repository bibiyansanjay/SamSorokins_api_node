import { JotformSubmission } from "../../models/index.js";

const getFieldValue = (answers, fieldName, keyName = "answer") => {
  const field = Object.values(answers || {}).find(
    (item) => item?.text === fieldName
  );
  if (!field) return null;
  const value = field?.[keyName] ?? null;
  if (typeof value === "string") return value.replace(/<[^>]*>/g, "").trim();
  return value;
};

export default async (req, res, next) => {
  try {
    const {
      search = "",
      status = "",
      propertyName = "",
      unitName = "",
      page = 0,
      pageSize = 50,
      sortField = "createdAt",
      sortOrder = "desc",
    } = req.query;

    const skip = Number(page) * Number(pageSize);
    const limit = Number(pageSize);
    const sortDir = sortOrder === "asc" ? 1 : -1;

    // ── Base match filter for submissions requiring files ──
    const matchRequiresUpload = {
      $match: {
        $expr: {
          $gt: [
            {
              $size: {
                $filter: {
                  input: { $objectToArray: { $ifNull: ["$answers", {}] } },
                  as: "item",
                  cond: {
                    $and: [
                      { $eq: ["$$item.v.text", "Files to Upload"] },
                      { $isArray: "$$item.v.answer" },
                      { $gt: [{ $size: "$$item.v.answer" }, 0] },
                    ],
                  },
                },
              },
            },
            0,
          ],
        },
      },
    };

    const lookupStage = {
      $lookup: {
        from: "uploads",
        localField: "submissionId",
        foreignField: "submissionId",
        as: "files",
      },
    };

    const addOverallStatusStage = {
      $addFields: {
        overallStatus: {
          $cond: {
            if: { $eq: [{ $size: "$files" }, 0] },
            then: "Pending",
            else: {
              $cond: {
                if: {
                  $allElementsTrue: {
                    $map: {
                      input: "$files",
                      as: "f",
                      in: { $eq: ["$$f.status", "Uploaded"] },
                    },
                  },
                },
                then: "Uploaded",
                else: {
                  $cond: {
                    if: {
                      $anyElementTrue: {
                        $map: {
                          input: "$files",
                          as: "f",
                          in: { $eq: ["$$f.status", "Uploading"] },
                        },
                      },
                    },
                    then: "Uploading",
                    else: {
                      $cond: {
                        if: {
                          $anyElementTrue: {
                            $map: {
                              input: "$files",
                              as: "f",
                              in: { $eq: ["$$f.status", "Failed"] },
                            },
                          },
                        },
                        then: "Failed",
                        else: "Pending",
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    };

    // Extract dynamic Jotform answer fields into top-level properties so MongoDB can sort by them globally
    const extractFieldsForSortStage = {
      $addFields: {
        answersListForSort: { $objectToArray: { $ifNull: ["$answers", {}] } },
      },
    };

    const projectSortFieldsStage = {
      $addFields: {
        extractedUnitName: {
          $let: {
            vars: {
              target: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: "$answersListForSort",
                      as: "item",
                      cond: { $eq: ["$$item.v.text", "RM Unit Name"] },
                    },
                  },
                  0,
                ],
              },
            },
            in: "$$target.v.answer",
          },
        },
        extractedClientName: {
          $let: {
            vars: {
              target: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: "$answersListForSort",
                      as: "item",
                      cond: { $eq: ["$$item.v.text", "User Name"] },
                    },
                  },
                  0,
                ],
              },
            },
            in: "$$target.v.answer",
          },
        },
        extractedClientEmail: {
          $let: {
            vars: {
              target: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: "$answersListForSort",
                      as: "item",
                      cond: { $eq: ["$$item.v.text", "User Email"] },
                    },
                  },
                  0,
                ],
              },
            },
            in: "$$target.v.answer",
          },
        },
        extractedPropertyName: {
          $let: {
            vars: {
              target: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: "$answersListForSort",
                      as: "item",
                      cond: { $eq: ["$$item.v.text", "RM Short Name"] },
                    },
                  },
                  0,
                ],
              },
            },
            in: "$$target.v.answer",
          },
        },
        extractedFormName: {
          $let: {
            vars: {
              target: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: "$answersListForSort",
                      as: "item",
                      cond: { $eq: ["$$item.v.text", "Uploader Header"] },
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
    };

    // Determine dynamic sort order
    let dynamicSort = { _id: sortDir };
    if (sortField === "createdAt") {
      dynamicSort = { createdAt: sortDir, _id: sortDir };
    } else if (sortField === "submissionId") {
      dynamicSort = { submissionId: sortDir, _id: sortDir };
    } else if (sortField === "status") {
      dynamicSort = { overallStatus: sortDir, _id: sortDir };
    } else if (sortField === "unitName") {
      dynamicSort = { extractedUnitName: sortDir, _id: sortDir };
    } else if (sortField === "clientName") {
      dynamicSort = { extractedClientName: sortDir, _id: sortDir };
    } else if (sortField === "clientEmail") {
      dynamicSort = { extractedClientEmail: sortDir, _id: sortDir };
    } else if (sortField === "propertyName") {
      dynamicSort = { extractedPropertyName: sortDir, _id: sortDir };
    } else if (sortField === "formName") {
      dynamicSort = { extractedFormName: sortDir, _id: sortDir };
    }

    let grouped = [];
    let totalFiles = 0;

    const pipeline = [
      matchRequiresUpload,
      lookupStage,
      addOverallStatusStage,
      extractFieldsForSortStage,
      projectSortFieldsStage,
    ];

    if (status) {
      pipeline.push({ $match: { overallStatus: status } });
    }

    if (propertyName && propertyName !== "All") {
      pipeline.push({ $match: { extractedPropertyName: propertyName } });
    }

    if (unitName && unitName !== "All") {
      pipeline.push({ $match: { extractedUnitName: unitName } });
    }

    if (search) {
      pipeline.push({
        $match: {
          $or: [
            { submissionId: { $regex: search, $options: "i" } },
            {
              "answersListForSort.v.answer": { $regex: search, $options: "i" },
            },
            { "files.filename": { $regex: search, $options: "i" } },
            { "files.residentName": { $regex: search, $options: "i" } },
            { "files.residentEmail": { $regex: search, $options: "i" } },
            { "files.status": { $regex: search, $options: "i" } },
            { overallStatus: { $regex: search, $options: "i" } },
          ],
        },
      });
    }

    const countPipeline = [...pipeline, { $count: "total" }];
    const [countResult] = await JotformSubmission.aggregate(countPipeline);
    totalFiles = countResult?.total || 0;

    pipeline.push({ $sort: dynamicSort });
    pipeline.push({ $skip: skip });
    pipeline.push({ $limit: limit });

    grouped = await JotformSubmission.aggregate(pipeline);

    // ── Format response ───────────────────────────────────────────────────
    const formatted = grouped.map((item) => {
      const answers = item.answers || {};

      const files = (item.files || []).map((f) => ({
        _id: f._id,
        filename: f.filename,
        s3Url: f.s3Url,
        status: f.status,
        createdAt: f.createdAt,
      }));

      const filesToUploadField = Object.values(answers).find(
        (item) => item?.text === "Files to Upload"
      );
      const requiresUpload =
        filesToUploadField &&
        Array.isArray(filesToUploadField.answer) &&
        filesToUploadField.answer.length > 0;

      let overallStatus;
      if (!requiresUpload) {
        overallStatus = "Upload not required";
      } else if (files.length === 0 || !item?.isSubmited) {
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
        _id: item._id,
        submissionId: item.submissionId,
        createdAt: item.createdAt,
        status: overallStatus,
        clientName: getFieldValue(answers, "User Name"),
        clientEmail: getFieldValue(answers, "User Email"),
        propertyName: getFieldValue(answers, "RM Short Name"),
        unitName: getFieldValue(answers, "RM Unit Name"),
        clientAddress: getFieldValue(answers, "Full Address"),
        formName: getFieldValue(answers, "Uploader Header"),
        files,
        totalFiles: files.length,
      };
    });

    return res.json({
      message: "Submissions fetched successfully",
      submissions: formatted,
      total: totalFiles,
    });
  } catch (error) {
    console.error("[submissionsData/get]", error);
    next(error);
  }
};

// import { JotformSubmission } from "../../models/index.js";

// const getFieldValue = (answers, fieldName, keyName = "answer") => {
//   const field = Object.values(answers || {}).find(
//     (item) => item?.text === fieldName
//   );
//   if (!field) return null;
//   const value = field?.[keyName] ?? null;
//   if (typeof value === "string") return value.replace(/<[^>]*>/g, "").trim();
//   return value;
// };

// export default async (req, res, next) => {
//   try {
//     const {
//       search = "",
//       status = "",
//       page = 0,
//       pageSize = 50,
//       sortField = "createdAt",
//       sortOrder = "desc",
//     } = req.query;

//     const skip = Number(page) * Number(pageSize);
//     const limit = Number(pageSize);
//     const sortDir = sortOrder === "asc" ? 1 : -1;

//     // ── Exact original match filter for submissions requiring files ──
//     const matchRequiresUpload = {
//       $match: {
//         $expr: {
//           $gt: [
//             {
//               $size: {
//                 $filter: {
//                   input: { $objectToArray: { $ifNull: ["$answers", {}] } },
//                   as: "item",
//                   cond: {
//                     $and: [
//                       { $eq: ["$$item.v.text", "Files to Upload"] },
//                       { $isArray: "$$item.v.answer" },
//                       { $gt: [{ $size: "$$item.v.answer" }, 0] },
//                     ],
//                   },
//                 },
//               },
//             },
//             0,
//           ],
//         },
//       },
//     };

//     const lookupStage = {
//       $lookup: {
//         from: "uploads",
//         localField: "submissionId",
//         foreignField: "submissionId",
//         as: "files",
//       },
//     };

//     const addOverallStatusStage = {
//       $addFields: {
//         overallStatus: {
//           $cond: {
//             if: { $eq: [{ $size: "$files" }, 0] },
//             then: "Pending",
//             else: {
//               $cond: {
//                 if: {
//                   $allElementsTrue: {
//                     $map: {
//                       input: "$files",
//                       as: "f",
//                       in: { $eq: ["$$f.status", "Uploaded"] },
//                     },
//                   },
//                 },
//                 then: "Uploaded",
//                 else: {
//                   $cond: {
//                     if: {
//                       $anyElementTrue: {
//                         $map: {
//                           input: "$files",
//                           as: "f",
//                           in: { $eq: ["$$f.status", "Uploading"] },
//                         },
//                       },
//                     },
//                     then: "Uploading",
//                     else: {
//                       $cond: {
//                         if: {
//                           $anyElementTrue: {
//                             $map: {
//                               input: "$files",
//                               as: "f",
//                               in: { $eq: ["$$f.status", "Failed"] },
//                             },
//                           },
//                         },
//                         then: "Failed",
//                         else: "Pending",
//                       },
//                     },
//                   },
//                 },
//               },
//             },
//           },
//         },
//       },
//     };

//     let grouped = [];
//     let totalFiles = 0;

//     const hasStatusFilter = Boolean(status);
//     const hasSearchFilter = Boolean(search);

//     if (!hasStatusFilter && !hasSearchFilter) {
//       // FAST PATH: Count & Paginate BEFORE Lookup/AddFields
//       const countPipeline = [matchRequiresUpload, { $count: "total" }];
//       const [countResult] = await JotformSubmission.aggregate(countPipeline);
//       totalFiles = countResult?.total || 0;

//       const fastPipeline = [
//         matchRequiresUpload,
//         { $sort: { _id: sortDir } },
//         { $skip: skip },
//         { $limit: limit },
//         lookupStage,
//         addOverallStatusStage,
//       ];

//       grouped = await JotformSubmission.aggregate(fastPipeline);
//     } else {
//       // FULL PATH: Used only when filtering by specific overallStatus or search text
//       const pipeline = [matchRequiresUpload, lookupStage, addOverallStatusStage];

//       if (status) {
//         pipeline.push({ $match: { overallStatus: status } });
//       }

//       if (search) {
//         pipeline.push({
//           $addFields: {
//             answersArray: { $objectToArray: { $ifNull: ["$answers", {}] } },
//           },
//         });
//         pipeline.push({
//           $match: {
//             $or: [
//               { submissionId: { $regex: search, $options: "i" } },
//               { "answersArray.v.answer": { $regex: search, $options: "i" } },
//               { "files.filename": { $regex: search, $options: "i" } },
//               { "files.residentName": { $regex: search, $options: "i" } },
//               { "files.residentEmail": { $regex: search, $options: "i" } },
//               { "files.status": { $regex: search, $options: "i" } },
//               { overallStatus: { $regex: search, $options: "i" } },
//             ],
//           },
//         });
//       }

//       const countPipeline = [...pipeline, { $count: "total" }];
//       const [countResult] = await JotformSubmission.aggregate(countPipeline);
//       totalFiles = countResult?.total || 0;

//       pipeline.push({ $sort: { _id: sortDir } });
//       pipeline.push({ $skip: skip });
//       pipeline.push({ $limit: limit });

//       grouped = await JotformSubmission.aggregate(pipeline);
//     }

//     // ── Format response ───────────────────────────────────────────────────
//     const formatted = grouped.map((item) => {
//       const answers = item.answers || {};

//       const files = (item.files || []).map((f) => ({
//         _id: f._id,
//         filename: f.filename,
//         s3Url: f.s3Url,
//         status: f.status,
//         createdAt: f.createdAt,
//       }));

//       const filesToUploadField = Object.values(answers).find(
//         (item) => item?.text === "Files to Upload"
//       );
//       const requiresUpload =
//         filesToUploadField &&
//         Array.isArray(filesToUploadField.answer) &&
//         filesToUploadField.answer.length > 0;

//       let overallStatus;
//       if (!requiresUpload) {
//         overallStatus = "Upload not required";
//       } else if (files.length === 0 || !item?.isSubmited) {
//         overallStatus = "Pending";
//       } else {
//         const allUploaded = files.every((f) => f.status === "Uploaded");
//         const anyUploading = files.some((f) => f.status === "Uploading");
//         const anyFailed = files.some((f) => f.status === "Failed");

//         if (allUploaded) overallStatus = "Uploaded";
//         else if (anyUploading) overallStatus = "Uploading";
//         else if (anyFailed) overallStatus = "Failed";
//         else overallStatus = "Pending";
//       }

//       return {
//         _id: item._id,
//         submissionId: item.submissionId,
//         createdAt: item.createdAt,
//         status: overallStatus,
//         clientName: getFieldValue(answers, "User Name"),
//         clientEmail: getFieldValue(answers, "User Email"),
//         propertyName: getFieldValue(answers, "RM Short Name"),
//         unitName: getFieldValue(answers, "RM Unit Name"),
//         clientAddress: getFieldValue(answers, "Full Address"),
//         formName: getFieldValue(answers, "Uploader Header"),
//         files,
//         totalFiles: files.length,
//       };
//     });

//     return res.json({
//       message: "Submissions fetched successfully",
//       submissions: formatted,
//       total: totalFiles,
//     });
//   } catch (error) {
//     console.error("[submissionsData/get]", error);
//     next(error);
//   }
// };
