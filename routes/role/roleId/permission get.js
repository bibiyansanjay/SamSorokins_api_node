import pool from "../../../db";

export default async (req, res, next) => {
  try {
    const { roleId } = req;

    if (!roleId) {
      return res.status(400).json({
        success: false,
        message: "Missing role_id in request",
      });
    }

    const sql = `
      SELECT 
        rp.*,
        r.name AS role_name,
        m.name AS module_name
      FROM \`role_permission\` rp
      LEFT JOIN \`role\` r ON rp.role_id = r.id
      LEFT JOIN \`module\` m ON rp.module_id = m.id
      WHERE rp.role_id = ?
      ORDER BY 
        (rp.add + rp.view + rp.edit + rp.delete) DESC,
        rp.module_id ASC
    `;

    const [rows] = await pool.query(sql, [roleId]);

    return res.status(200).json({
      success: true,
      message: "Role permissions fetched successfully",
      data: rows,
    });
  } catch (error) {
    console.error("GET /role-permissions - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching role permissions",
      error: error.message,
    });
  }
};
