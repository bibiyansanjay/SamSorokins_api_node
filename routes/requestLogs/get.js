import { RequestLog, User } from "../../models/index.js";

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
    const sortDir = sortOrder === "asc" ? 1 : -1;

    let query = {};

    if (search && search.trim()) {
      const searchRegex = new RegExp(search.trim(), "i");
      query.$or = [
        { requestName: searchRegex },
        { email: searchRegex },
        { ipAddress: searchRegex },
        { method: searchRegex },
        { url: searchRegex },
      ];

      const parsedStatus = parseInt(search.trim(), 10);
      if (!isNaN(parsedStatus)) {
        query.$or.push({ statusCode: parsedStatus });
      }
    }

    const sortOptions = {};
    if (sortField) {
      sortOptions[sortField] = sortDir;
    } else {
      sortOptions.createdAt = -1;
    }

    const [logs, total] = await Promise.all([
      RequestLog.find(query)
        .populate("userId", "firstName lastName fullName email role")
        .sort(sortOptions)
        .skip(skip)
        .limit(limit)
        .lean(),
      RequestLog.countDocuments(query),
    ]);

    const formattedLogs = logs.map((log) => ({
      _id: log._id,
      requestName: log.requestName || "API Request",
      userId: log.userId?._id || log.userId || null,
      userName:
        log.userId?.fullName ||
        (log.userId?.firstName
          ? `${log.userId.firstName} ${log.userId.lastName || ""}`.trim()
          : ""),
      userRole: log.userId?.role || "",
      email: log.email || log.userId?.email || "-",
      ipAddress: log.ipAddress || "-",
      method: log.method || "-",
      url: log.url || "-",
      statusCode: log.statusCode || 200,
      responseTime: log.responseTime || 0,
      userAgent: log.userAgent || "",
      createdAt: log.createdAt,
    }));

    return res.json({
      message: "Request logs fetched successfully",
      logs: formattedLogs,
      total,
    });
  } catch (error) {
    console.error("[requestLogs/get]", error);
    next(error);
  }
};
