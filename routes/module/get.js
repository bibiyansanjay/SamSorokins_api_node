import pool from "../../db";

export default async (req, res, next) => {
  try {
    const userRole = req.user.role;
    //console.log("userRole:", userRole);
    // Super Admin Developer --- send all modules
    if (userRole === 6) {
      const sql = `
        SELECT id, name as module_name, isActive FROM module ORDER BY id ASC
      `;
      const [moduleRows] = await pool.query(sql);

      return res.status(200).json({
        success: true,
        message:
          moduleRows.length === 0
            ? "No records found"
            : "Module data fetched successfully",
        data: moduleRows,
      });
    }
    // Admin --- send all active modules and non-deleted modules
    else if (userRole === 1) {
      const sql = `
        SELECT id, name as module_name, isActive FROM module WHERE isActive = 1 AND isDeleted = 0 ORDER BY id ASC
      `;
      const [moduleRows] = await pool.query(sql);

      return res.status(200).json({
        success: true,
        message:
          moduleRows.length === 0
            ? "No records found"
            : "Module data fetched successfully",
        data: moduleRows,
      });
    }
    // For client role, send only their own module
    else {
      const sql = `
    SELECT m.id, m.isActive, m.name AS module_name
    FROM \`role_permission\` rp
    JOIN \`module\` m ON rp.module_id = m.id
    WHERE rp.role_id = ?
      AND m.isActive = 1
      AND m.isDeleted = 0
      AND (
        rp.add = 1 OR
        rp.view = 1 OR
        rp.edit = 1 OR
        rp.delete = 1
      )
        ORDER BY m.id ASC`;

      const [moduleRows] = await pool.query(sql, [userRole]);

      return res.status(200).json({
        success: true,
        message:
          moduleRows.length === 0
            ? "No records found"
            : "Module data fetched successfully",
        data: moduleRows,
      });
    }
  } catch (error) {
    console.error("GET /modules - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching modules",
      error: error.message,
    });
  }
};
