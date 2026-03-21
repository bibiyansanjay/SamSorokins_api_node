import { JotformSubmission } from "../../models/index.js";
import uploads from "../../models/uploads/index.js";

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
      pageSize = 12,
      sortField = "createdAt",
      sortOrder = "desc",
    } = req.query;

    const skip = Number(page) * Number(pageSize);
    const limit = Number(pageSize);
    const sort = { [sortField]: sortOrder === "asc" ? 1 : -1 };

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
      pipeline.push({
        $match: {
          $or: [
            { submissionId: { $regex: search, $options: "i" } },
            { "files.residentName": { $regex: search, $options: "i" } },
            { "files.residentEmail": { $regex: search, $options: "i" } },
          ],
        },
      });
    }

    // ── Count total (before pagination) ──────────────────────────────────
    const countPipeline = [...pipeline, { $count: "total" }];
    const [countResult] = await JotformSubmission.aggregate(countPipeline);
    const totalFiles = countResult?.total || 0;

    // ── Sort + paginate ───────────────────────────────────────────────────
    pipeline.push({ $sort: sort });
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
        email: getFieldValue(answers, "Email"),
        files,
        totalFiles: files.length,
        formData: {
          Name: getFieldValue(answers, "User Name"),
          Email: getFieldValue(answers, "Email"),
          Phone: getFieldValue(answers, "User Phone Number"),
          Address: getFieldValue(answers, "Full Address"),
        },
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
