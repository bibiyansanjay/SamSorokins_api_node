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
                  SELECT 
                      u.id,
                      u.page as page_id,
                      p.title as page_title,
                      p.key as page_key,
                      u.title,
                      u.description,
                      u.video_url,
                      u.category as category_id,
                      vc.name as category_name, 
                      u.display_order,
                      u.duration_seconds, 
                      u.language_code,
                      u.thumbnail_url,
                      u.isActive
                    FROM \`videos\` u
                    LEFT JOIN \`pages\` p ON p.id= u.page
                    LEFT JOIN \`video_categories\` vc ON vc.id= u.category
                  WHERE u.isDeleted = false
                `;
      // Fire both queries at once (uses 2 pool connections)
      const [[rowsResult]] = await Promise.all([pool.query(sql)]);
      // console.log("rowsResult:", rowsResult);
      return res.status(200).json({
        success: true,
        message:
          rowsResult.length === 0
            ? "No records found"
            : "Videos data fetched successfully",
        videos: rowsResult,
      });
    }
  } catch (error) {
    console.error("GET /videos - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching videos",
      error: error.message,
    });
  }
};
