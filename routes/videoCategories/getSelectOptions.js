import pool from "../../db";

export default async (req, res, next) => {
  try {
    const userRole = req.user.role;
    //console.log("userRole:", userRole);

    if (userRole != "1" && userRole != "6") {
      return res.status(500).json({
        success: false,
        message: "Un-Authrorized Access",
      });
    } else {
      const sql = `
      SELECT id as value, name as label
      FROM \`video_categories\` p
      ORDER BY p.id ASC
    `;

      // Fire both queries at once (uses 2 pool connections)
      const [[rowsResult]] = await Promise.all([pool.query(sql)]);

      return res.status(200).json({
        success: true,
        message:
          rowsResult.length === 0
            ? "No records found"
            : "Video Categories data fetched successfully",
        video_categories: rowsResult,
      });
    }
  } catch (error) {
    console.error("GET /Video Categories - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching Video Categories",
      error: error.message,
    });
  }
};
