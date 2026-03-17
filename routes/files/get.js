// import { Upload } from "../../models";

// export default async (req, res, next) => {
//   try {
//     const { search, page = 0, pageSize = 12, sortField, sortOrder } = req.query;

//     const skip = page * pageSize;

//     // Search filter
//     const searchFilter = search
//       ? {
//           $or: [
//             { filename: { $regex: search, $options: "i" } },
//             { residentName: { $regex: search, $options: "i" } },
//           ],
//         }
//       : {};

//     const filter = {
//       ...searchFilter,
//     };

//     // Sorting
//     const sort = {};
//     if (sortField) {
//       sort[sortField] = sortOrder === "asc" ? 1 : -1;
//     }

//     const [files, totalFiles] = await Promise.all([
//       Upload.find(filter).sort(sort).skip(skip).limit(Number(pageSize)),
//       Upload.countDocuments(filter),
//     ]);

//     return res.json({
//       message: "Files fetched successfully",
//       files,
//       totalFiles,
//     });
//   } catch (error) {
//     console.error(error);
//     next(error);
//   }
// };

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

    // Count total grouped submissions
    const totalFilesAgg = await Upload.aggregate([
      { $match: searchFilter },
      {
        $group: {
          _id: "$submissionId",
        },
      },
      {
        $count: "total",
      },
    ]);

    const totalFiles = totalFilesAgg[0]?.total || 0;

    const formatted = grouped.map((item) => ({
      _id: item.submissionId,
      submissionId: item.submissionId,
      createdAt: item.createdAt,

      clientName: getFieldValue(item.answers, "Full Name"),

      email: getFieldValue(item.answers, "Email"),

      files: item.files.map((f) => ({
        filename: f.filename,
        url: f.url,
      })),

      formData: {
        residentName: getFieldValue(item.answers, "Full Name"),
        email: getFieldValue(item.answers, "Email"),
        phone: getFieldValue(item.answers, "Phone"),
      },
    }));

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
