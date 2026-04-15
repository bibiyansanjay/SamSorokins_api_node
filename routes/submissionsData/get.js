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
      page = 0,
      pageSize = 50,
      sortField = "createdAt",
      sortOrder = "desc",
    } = req.query;

    const skip = Number(page) * Number(pageSize);
    const limit = Number(pageSize);

    // ── Base aggregation: only submissions requiring uploads + join Uploads ──
    const pipeline = [
      {
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
      },
      {
        $lookup: {
          from: "uploads",
          localField: "submissionId",
          foreignField: "submissionId",
          as: "files",
        },
      },
    ];

    // ── Optional search ───────────────────────────────────────────────────
    if (search) {
      // First, convert the dynamic `answers` object into an array so we can search inside its values
      pipeline.push({
        $addFields: {
          answersArray: { $objectToArray: { $ifNull: ["$answers", {}] } },
        },
      });

      pipeline.push({
        $match: {
          $or: [
            // Search inside the submission ID itself
            { submissionId: { $regex: search, $options: "i" } },
            // Search ANY dynamic value inside the JotForm answers (covers names, emails, unit names, properties, etc.)
            { "answersArray.v.answer": { $regex: search, $options: "i" } },
            // Search against the joined uploaded files' data
            { "files.filename": { $regex: search, $options: "i" } },
            { "files.residentName": { $regex: search, $options: "i" } },
            { "files.residentEmail": { $regex: search, $options: "i" } },
            { "files.status": { $regex: search, $options: "i" } },
          ],
        },
      });
    }

    // ── Count total (before pagination) ──────────────────────────────────
    const countPipeline = [...pipeline, { $count: "total" }];
    const [countResult] = await JotformSubmission.aggregate(countPipeline);
    const totalFiles = countResult?.total || 0;

    // ── Parse Date for Sorting ────────────────────────────────────────────
    pipeline.push({
      $addFields: {
        dateParts: {
          $cond: {
            if: { $eq: [{ $type: "$createdAt" }, "string"] },
            then: { $split: ["$createdAt", " "] },
            else: null,
          },
        },
      },
    });

    // pipeline.push({
    //   $addFields: {
    //     parsedCreatedAt: {
    //       $cond: {
    //         if: { $eq: [{ $type: "$createdAt" }, "string"] },
    //         then: {
    //           $convert: {
    //             input: {
    //               $concat: [
    //                 { $arrayElemAt: ["$dateParts", 3] }, // YYYY
    //                 "-",
    //                 {
    //                   $switch: {
    //                     branches: [
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Jan"],
    //                         },
    //                         then: "01",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Feb"],
    //                         },
    //                         then: "02",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Mar"],
    //                         },
    //                         then: "03",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Apr"],
    //                         },
    //                         then: "04",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "May"],
    //                         },
    //                         then: "05",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Jun"],
    //                         },
    //                         then: "06",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Jul"],
    //                         },
    //                         then: "07",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Aug"],
    //                         },
    //                         then: "08",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Sep"],
    //                         },
    //                         then: "09",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Oct"],
    //                         },
    //                         then: "10",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Nov"],
    //                         },
    //                         then: "11",
    //                       },
    //                       {
    //                         case: {
    //                           $eq: [{ $arrayElemAt: ["$dateParts", 1] }, "Dec"],
    //                         },
    //                         then: "12",
    //                       },
    //                     ],
    //                     default: "01",
    //                   },
    //                 },
    //                 "-",
    //                 { $arrayElemAt: ["$dateParts", 2] }, // DD
    //                 "T",
    //                 { $arrayElemAt: ["$dateParts", 4] }, // HH:MM:SS
    //                 "Z",
    //               ],
    //             },
    //             to: "date",
    //             onError: new Date(0),
    //             onNull: new Date(0),
    //           },
    //         },
    //         else: "$createdAt",
    //       },
    //     },
    //   },
    // });

    // ── Sort + paginate ───────────────────────────────────────────────────
    const safeSort = { _id: sortOrder === "asc" ? 1 : -1 };

    pipeline.push({ $sort: safeSort });
    pipeline.push({ $skip: skip });
    pipeline.push({ $limit: limit });

    const grouped = await JotformSubmission.aggregate(pipeline);

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

      // Overall status logic:
      // 1. "Files to Upload" answer doesn't exist → "Upload not required"
      // 2. No upload records at all            → "Pending"
      // 3. Has uploads → derive from their statuses
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
      } else if (files.length === 0) {
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
        clientEmail: getFieldValue(answers, "Email"),
        propertyName: getFieldValue(answers, "RM Short Name"), //Property
        unitName: getFieldValue(answers, "RM Unit Name"), //Unit
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
