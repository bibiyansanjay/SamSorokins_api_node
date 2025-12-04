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
      // Define both SQL queries
      const sqlPages = `
      SELECT 
        u.id,
        u.title,
        u.key,
        u.description
      FROM \`pages\` u
    `;

      const sqlVideos = `
      SELECT 
        v.id,
        v.page AS page_id,
        p.title AS page_title,
        p.key AS page_key,
        v.title,
        v.description,
        v.video_url,
        v.category AS category_id,
        vc.name AS category_name,
        v.display_order,
        v.duration_seconds,
        v.language_code,
        v.thumbnail_url,
        v.isActive
      FROM \`videos\` v
      LEFT JOIN \`pages\` p ON p.id = v.page
      LEFT JOIN \`video_categories\` vc ON vc.id = v.category
      WHERE v.isDeleted = 0
    `;

      // Run both queries in parallel
      const [[pages], [videos]] = await Promise.all([
        pool.query(sqlPages),
        pool.query(sqlVideos),
      ]);

      // Combine data: attach videos to their corresponding page
      const pagesWithVideos = pages.map((page) => {
        const relatedVideos = videos.filter((v) => v.page_id === page.id);
        return {
          ...page,
          videos: relatedVideos,
        };
      });

      // Send response
      return res.status(200).json({
        success: true,
        message:
          pagesWithVideos.length === 0
            ? "No records found"
            : "Pages and videos data fetched successfully",
        data: pagesWithVideos,
      });
    }
  } catch (error) {
    console.error("GET /Pages Videos - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching Pages",
      error: error.message,
    });
  }
};
