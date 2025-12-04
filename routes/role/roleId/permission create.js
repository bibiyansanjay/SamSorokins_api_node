import pool from "../../../db"; // assuming mysql2 pool is exported from here

export default async (req, res) => {
  try {
    const permissions = req.body;
    //console.log("POST /role-permissions - Saving permissions:", permissions);
    if (!Array.isArray(permissions) || permissions.length === 0) {
      return res.status(400).json({
        success: false,
        message: "Invalid or empty permissions array provided.",
      });
    }

    // Build the placeholders for bulk insert
    const placeholders = permissions.map(() => "(?, ?, ?, ?, ?, ?)").join(", ");

    // Flatten all values into a single array
    const values = permissions.flatMap(
      ({ role_id, module_id, add, view, edit, delete: del }) => [
        role_id,
        module_id,
        add,
        view,
        edit,
        del,
      ]
    );

    const sql = `
      INSERT INTO role_permission 
        (role_id, module_id, \`add\`, \`view\`, \`edit\`, \`delete\`)
      VALUES ${placeholders}
      ON DUPLICATE KEY UPDATE 
        \`add\` = VALUES(\`add\`),
        \`view\` = VALUES(\`view\`),
        \`edit\` = VALUES(\`edit\`),
        \`delete\` = VALUES(\`delete\`);
    `;
    //console.log("SQL Query:", sql);
    //console.log("Values:", values);
    const [result] = await pool.query(sql, values);

    return res.status(200).json({
      success: true,
      message: "Role permissions saved successfully",
      result,
    });
  } catch (error) {
    console.error("POST /role-permissions - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while saving role permissions",
      error: error.message,
    });
  }
};
