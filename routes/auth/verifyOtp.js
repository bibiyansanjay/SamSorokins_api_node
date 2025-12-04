import Joi from "joi";
import getToken from "../../models/users/methods/getToken";
import updateRecord from "../../utils/updateRecord";
import getRecord from "../../utils/getRecord";
import pool from "../../db"; // assuming mysql2 pool is exported from here

const schema = Joi.object({
  email: Joi.string().required(),
  otp: Joi.string().required(),
});

export default async (req, res, next) => {
  try {
    const { body } = req;
    const data = await schema.validateAsync(body);
    const { email, otp } = data;

    //1. get user by email - Fetch the updated user data (same as GET /user/:id)
    const sqlUser = `
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
      WHERE u.email = ? AND u.isDeleted = false
    `;

    const [userRows] = await pool.query(sqlUser, [email]);

    if (!userRows || userRows.length === 0) {
      return res.status(404).json({
        success: false,
        message: "User not found",
        data: [],
      });
    }

    const user = userRows[0];
    //console.log("Verify OTP - User found", user);
    if (user.isDeleted === 1 || user.isActive === 0) {
      return res.status(403).json({ message: "User is inactive or deleted" });
    }
    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }
    if (user?.otp !== otp) {
      return res.status(400).json({ message: "Invalid OTP" });
    }
    //get the parent logo for loggedinUser-------------starts
    if (user.role == "1" || user.role == "6" || user.role == "2") {
      user["companyLogo"] = user.usercompanyLogo;
    } else {
      user["companyLogo"] = user.parentcompanyLogo;
    }
    //get the parent logo for loggedinUser-------------ends
    const token = getToken(user.id);
    const now = new Date();
    // Reset resend count & clear OTP after successful login
    const result = await updateRecord(
      "user",
      {
        otp: null,
        resendOtpCount: 0,
        lastActive: now,
      },
      { email: user.email }
    );
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Verify OTP - user update failed" });
    }
    //console.log("Verify OTP - user updated successfully");
    //
    //--------------------------------------------------------add permissions to user object
    const { roleId } = user.role;
    //2. if no roleId, set permissions to empty array
    if (!roleId) {
      user["permissions"] = [];
      //user["modules"] = [];
      return res.json({ message: "User Logged In Successfully", token, user });
    }
    //2. roleId is present, fetch permissions
    const sql = `
      SELECT
        rp.*,
        r.name AS role_name,
        m.name AS module_name
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
    const [permissionsRows] = await pool.query(sql, [roleId]);
    user["permissions"] = permissionsRows;

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

    const [moduleRows] = await pool.query(sql3, [roleId]);
    const moduleNames = moduleRows.map((row) => row.module_name);

    user["modules"] = moduleNames;
    */
    return res.json({ message: "User Logged In Successfully", token, user });
    //--------------------------------------------------------add permissions to user object
  } catch (error) {
    console.log("Post - Error while verifying otp", error);
    next(error);
  }
};
