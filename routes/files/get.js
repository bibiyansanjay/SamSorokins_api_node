import { Upload } from "../../models";

const getFieldValue = (answers, fieldName, keyName = "answer") => {
  const field = Object.values(answers || {}).find(
    (item) => item.text === fieldName
  );

  if (!field) return null;

  const value = field?.[keyName] || null;

  if (typeof value === "string") {
    return value.replace(/<[^>]*>/g, "").trim();
  }

  return value;
};

export default async (req, res, next) => {
  try {
    const {
      search,
      page = 0,
      pageSize = 12,
      sortField = "createdAt",
      sortOrder = "desc",
    } = req.query;

    const skip = page * pageSize;

    const searchFilter = search
      ? {
          $or: [
            { filename: { $regex: search, $options: "i" } },
            { residentName: { $regex: search, $options: "i" } },
          ],
        }
      : {};

    const sort = { [sortField]: sortOrder === "asc" ? 1 : -1 };

    const grouped = await Upload.aggregate([
      { $match: searchFilter },

      { $sort: sort },

      {
        $group: {
          _id: "$submissionId",
          files: { $push: "$$ROOT" },
        },
      },

      {
        $lookup: {
          from: "jotformsubmissions",
          localField: "_id",
          foreignField: "submissionId",
          as: "submission",
        },
      },

      {
        $unwind: {
          path: "$submission",
          preserveNullAndEmptyArrays: true,
        },
      },

      {
        $project: {
          submissionId: "$_id",
          files: 1,
          createdAt: "$submission.createdAt",
          answers: "$submission.answers",
        },
      },

      { $skip: skip },
      { $limit: Number(pageSize) },
    ]);

    const totalFilesAgg = await Upload.aggregate([
      { $match: searchFilter },
      { $group: { _id: "$submissionId" } },
      { $count: "total" },
    ]);

    const totalFiles = totalFilesAgg[0]?.total || 0;

    const formatted = grouped.map((item) => {
      // ✅ Map files with their status included
      const files = item.files.map((f) => ({
        filename: f.filename,
        url: f.url,
        status: f.status,
      }));

      // ✅ If any file is Pending or Failed, overall status is "Pending"
      const hasPendingOrFailed = files.some(
        (f) => f.status === "Pending" || f.status === "Failed"
      );
      const status = hasPendingOrFailed ? "Pending" : "Uploaded";

      return {
        _id: item.submissionId,
        submissionId: item.submissionId,
        createdAt: item.createdAt,
        status, // ✅ Overall submission status
        clientName: getFieldValue(item.answers, "User Name"),
        email: getFieldValue(item.answers, "User Email"),
        files,
        formData: {
          residentName: getFieldValue(item.answers, "User Name"),
          email: getFieldValue(item.answers, "User Email"),
          phone: getFieldValue(item.answers, "Phone"),
        },
      };
    });

    return res.json({
      message: "Files fetched successfully",
      files: formatted,
      totalFiles,
    });
  } catch (error) {
    console.error(error);
    next(error);
  }
};
// export default async (req, res, next) => {
//   try {
//     const {
//       search,
//       page = 0,
//       pageSize = 12,
//       sortField = "createdAt",
//       sortOrder = "desc",
//     } = req.query;

//     const skip = page * pageSize;

//     const searchFilter = search
//       ? {
//           $or: [
//             { filename: { $regex: search, $options: "i" } },
//             { residentName: { $regex: search, $options: "i" } },
//           ],
//         }
//       : {};

//     const sort = { [sortField]: sortOrder === "asc" ? 1 : -1 };

//     const grouped = await Upload.aggregate([
//       { $match: searchFilter },

//       { $sort: sort },

//       {
//         $group: {
//           _id: "$submissionId",
//           files: { $push: "$$ROOT" },
//         },
//       },

//       {
//         $lookup: {
//           from: "jotformsubmissions",
//           localField: "_id",
//           foreignField: "submissionId",
//           as: "submission",
//         },
//       },

//       {
//         $unwind: {
//           path: "$submission",
//           preserveNullAndEmptyArrays: true,
//         },
//       },

//       {
//         $project: {
//           submissionId: "$_id",
//           files: 1,
//           createdAt: "$submission.createdAt",
//           answers: "$submission.answers",
//         },
//       },

//       { $skip: skip },
//       { $limit: Number(pageSize) },
//     ]);

//     // Count total grouped submissions
//     const totalFilesAgg = await Upload.aggregate([
//       { $match: searchFilter },
//       {
//         $group: {
//           _id: "$submissionId",
//         },
//       },
//       {
//         $count: "total",
//       },
//     ]);

//     const totalFiles = totalFilesAgg[0]?.total || 0;

//     const formatted = grouped.map((item) => ({
//       _id: item.submissionId,
//       submissionId: item.submissionId,
//       createdAt: item.createdAt,

//       clientName: getFieldValue(item.answers, "Full Name"),

//       email: getFieldValue(item.answers, "Email"),

//       files: item.files.map((f) => ({
//         filename: f.filename,
//         url: f.url,
//       })),

//       formData: {
//         residentName: getFieldValue(item.answers, "Full Name"),
//         email: getFieldValue(item.answers, "Email"),
//         phone: getFieldValue(item.answers, "Phone"),
//       },
//     }));

//     return res.json({
//       message: "Files fetched successfully",
//       files: formatted,
//       totalFiles,
//     });
//   } catch (error) {
//     console.error(error);
//     next(error);
//   }
// };
