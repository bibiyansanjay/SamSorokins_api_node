import Joi from "joi";
import updateRecord from "../../../utils/updateRecord";
import pool from "../../../db"; // assuming mysql2 pool is exported from here

const schema = Joi.object({
  firstName: Joi.string(),
  lastName: Joi.string(),
  email: Joi.string(),
  phoneNumber: Joi.string(),
  country: Joi.string(),
  state: Joi.string().allow(""),
  city: Joi.string().allow(""),
  address: Joi.string().allow(""),
  pinCode: Joi.string().allow(""),
  profileImg: Joi.string().allow(""),
  role: Joi.string(),
  preferredLanguage: Joi.string(),
  isDeleted: Joi.boolean(),
  isActive: Joi.boolean(),
  isLoggedIn: Joi.boolean(),
  gender: Joi.string().valid("male", "female", "other").optional(),
  signatureName: Joi.string(),
  signatureTeam: Joi.string(),
  signatureTitle: Joi.string(),
  signatureCompany: Joi.string(),
  companyLogo: Joi.string().allow(null),
});
export default async (req, res, next) => {
  try {
    const { body, userId } = req;
    const data = await schema.validateAsync(body);
    //console.log("data: ", data);
    // 1. Perform the update
    const result = await updateRecord("user", data, { id: userId });
    /*  
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "user not found or nothing was updated" });
    }
 */
    // 2. Fetch the updated user data (same as GET /user/:id)
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
      WHERE u.id = ? AND u.isDeleted = false
    `;
    const [userRows] = await pool.query(sqlUser, [userId]);

    if (!userRows || userRows.length === 0) {
      return res.status(404).json({
        success: false,
        message: "User updated, but not found on re-fetch",
        data: [],
      });
    }

    const user = userRows[0];

    // 3. Fetch permissions if user has a role
    if (user.role) {
      //get the parent logo for loggedinUser-------------starts
      if (user.role == "1" || user.role == "6" || user.role == "2") {
        user["companyLogo"] = user.usercompanyLogo;
      } else {
        user["companyLogo"] = user.parentcompanyLogo;
      }

      const sqlPermissions = `
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
      const [permissions] = await pool.query(sqlPermissions, [user.role]);
      user["permissions"] = permissions;
    } else {
      user["permissions"] = [];
    }
    //console.log("data: ", user);
    // 4. Return updated user object
    return res.status(200).json({
      success: true,
      message: "User Updated successfully",
      affectedRows: result.affectedRows,
      data: user,
    });
  } catch (error) {
    //next(error);
    console.error("Error in PUT /userId:", error.message);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
};
