import { Upload } from "../../../models";

export default async (req, res, next) => {
  try {
    const { search, page = 0, pageSize = 12, sortField, sortOrder } = req.query;
    const { submissionId } = req;

    console.log(
      { search },
      { page },
      { pageSize },
      { sortField },
      { sortOrder }
    );
    const skip = page * pageSize;

    // Search filter
    const searchFilter = search
      ? {
          $or: [
            { filename: { $regex: search, $options: "i" } },
            { residentName: { $regex: search, $options: "i" } },
          ],
        }
      : {};

    const filter = {
      submissionId,
      ...searchFilter,
    };

    // Sorting
    const sort = {};
    if (sortField) {
      sort[sortField] = sortOrder === "asc" ? 1 : -1;
    }

    const [files, totalFiles] = await Promise.all([
      Upload.find(filter).sort(sort).skip(skip).limit(Number(pageSize)),
      Upload.countDocuments(filter),
    ]);

    return res.json({
      message: "Files fetched successfully",
      files,
      totalFiles,
    });
  } catch (error) {
    console.error(error);
    next(error);
  }
};
