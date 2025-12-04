import pool from "../../../db"; // assuming mysql2 pool is exported from here

export default async (req, res, next) => {
  try {
    //console.log("GET /user/id - Fetching user by ID");
    //console.log("req:", req);
    const { userId } = req;
    //console.log("userId:", userId);

    //1. get user by id
    const sql1 = `
    SELECT 
        u.id,
        u.firstName,
        u.lastName,
        CONCAT(u.firstName, ' ', u.lastName) AS fullName,
        u.email,
        u.role,
        u.parent_id,
        u.phoneNumber,
        u.address, 
        u.city,
        u.country,
        u.state,
        u.pinCode,
        u.profileImg,
        u.gender,
        u.isActive,
        u.isDeleted,
        u.deleted_at,
        u.preferredLanguage,
        u.createdBy,
        u.created_at,
        u.otp,
        u.companyLogo AS usercompanyLogo,
        r.name AS role_name,
        CONCAT(cu.firstName, ' ', cu.lastName) AS parent_name,
        cu.companyLogo AS parentcompanyLogo
      FROM \`user\` u
      LEFT JOIN \`role\` r ON u.role = r.id
      LEFT JOIN \`user\` cu ON u.parent_id = cu.id
    WHERE u.id = ? AND u.isDeleted = false
  `;

    const [rows1] = await pool.query(sql1, [userId]);
    //console.log("rows1:", rows1);
    if (!rows1 || rows1.length === 0) {
      return res.status(404).json({
        success: false,
        message: "User not found",
        data: [],
      });
    }
    var user = rows1[0];
    //console.log("userby id - User found", user);
    //----------------------------------------------------------------------
    if (!user.role) {
      //console.log("userby id - user wd no Permissions", user);
      user["permissions"] = [];
      //user["modules"] = [];
      return res.status(200).json({
        success: true,
        message: "User data fetched successfully",
        data: user,
      });
    }
    //2. userId is present, fetch permissions
    //get the parent logo for loggedinUser-------------starts
    if (user.role == "1" || user.role == "6" || user.role == "2") {
      user["companyLogo"] = user.usercompanyLogo;
    } else {
      user["companyLogo"] = user.parentcompanyLogo;
    }
    const sql2 = `
      SELECT
        rp.*,
        r.name AS role_name,
        m.name AS module_name,
        m.value AS module_value
      FROM \`role_permission\` rp
      LEFT JOIN \`role\` r ON rp.role_id = r.id
      LEFT JOIN \`module\` m ON rp.module_id = m.id
      WHERE rp.role_id = ?
        AND m.isActive = 1
        AND m.isDeleted = 0
        AND (
          rp.add = 1 OR
          rp.view = 1 OR
          rp.edit = 1 OR
          rp.delete = 1
        )
      ORDER BY 
        (rp.add + rp.view + rp.edit + rp.delete) DESC,
        rp.module_id ASC
    `;

    const [permissionsRows] = await pool.query(sql2, [user.role]);
    user["permissions"] = permissionsRows;
    //console.log("userby id - user with Permissions fetched", user);

    //3. Fetch active modules for the user
    /*
    const sql3 = `
    SELECT m.name AS module_name
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
      )`;

    const [moduleRows] = await pool.query(sql3, [user.role]);
    const moduleNames = moduleRows.map((row) => row.module_name);

    user["modules"] = moduleNames; */
    //console.log("userby id - user with Modules fetched", user);
    //----------------------------------------------------------------------
    return res.status(200).json({
      success: true,
      message: "User data fetched successfully",
      data: user,
    });
  } catch (error) {
    console.error("GET /user/id - Error:", error);

    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching Users data",
      error: error.message,
    });
  }
};
