import { escapeRegex } from "../../methods/escapeRegex";
import { User } from "../../models";

export default async (req, res, next) => {
  try {
    const {
      query: { search, paginationModel, sortModel, showDeletedUsers },
      user,
    } = req;
    const isSearch = search && search.trim() !== "";
    const page = parseInt(paginationModel?.page) || 0;
    const pageSize = parseInt(paginationModel?.pageSize) || 10;
    let sorting = { createdAt: -1 };
    const query = {
      isDeleted: showDeletedUsers === "true" ? true : false,
    };

    if (pageSize) {
      query.role = { $ne: "ADMIN" };
    }

    if (sortModel && sortModel.length > 0) {
      const { field, sort } = sortModel[0];
      sorting = { [field]: sort === "asc" ? 1 : -1 };
    }
    // Apply user-specific filtering (Non-Admin users only see their payments)
    // if (user?.role !== "ADMIN") {
    //   query.createdBy = user?._id;
    // }
    if (isSearch) {
      const escapedSearch = escapeRegex(search);
      const searchRegex = new RegExp(escapedSearch, "i");
      query.$or = [
        { firstName: searchRegex },
        { lastName: searchRegex },
        { address: searchRegex },
        { gender: searchRegex },
        { role: searchRegex },
        { email: searchRegex },
        { phoneNumber: searchRegex },
      ];
    }

    const users = await User.find(query)
      // const users = await User.find({ isDeleted: false, role: { $ne: "ADMIN" } });

      .sort(sorting)
      .skip(page * pageSize)
      .limit(pageSize);

    const countQuery = {
      isDeleted: showDeletedUsers === "true" ? true : false,
    };

    if (isSearch) {
      const escapedSearch = escapeRegex(search);
      const searchRegex = new RegExp(escapedSearch, "i");
      countQuery.$or = [
        { firstName: searchRegex },
        { lastName: searchRegex },
        { address: searchRegex },
      ];
    }
    const userCount = await User.countDocuments(countQuery);

    return res.json({ message: "User Fetched Successfully", users, userCount });
    // return res.json({ message: "User Fetched Successfully", users });
  } catch (error) {
    next(error);
  }
};
