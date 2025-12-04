import pool from "../../../db";

export default async (req, res) => {
  try {
    const { moduleId } = req;

    if (!moduleId) {
      return res.status(400).json({
        success: false,
        message: "moduleId is required",
      });
    }

    // Fetch videos in display_order
    const [videos] = await pool.query(
      "SELECT * FROM videos WHERE page = ? ORDER BY display_order ASC",
      [moduleId]
    );

    return res.status(200).json({
      success: true,
      message: "Videos fetched successfully",
      data: videos,
    });
  } catch (error) {
    console.error("GET /Tutorial - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching module/tutorial data",
      error: error.message,
    });
  }
};
