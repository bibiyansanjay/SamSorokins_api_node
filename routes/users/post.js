import Joi from "joi";
import passwordHash from "../../models/users/pre/save/passwordHash";
import createRecord from "../../utils/createRecord";
import pool from "../../db";

const schema = Joi.object({
  firstName: Joi.string().required(),
  lastName: Joi.string().allow(""),
  email: Joi.string().required(),
  password: Joi.string().required(),
  address: Joi.string().allow(""),
  city: Joi.string().allow(""),
  country: Joi.string().required(),
  state: Joi.string().allow(""),
  phoneNumber: Joi.string(),
  pinCode: Joi.string().allow(""),
  profileImg: Joi.string().allow(""),
  role: Joi.string().required(),
  isDeleted: Joi.boolean(),
  isActive: Joi.boolean(),
  gender: Joi.string().valid("male", "female", "other").optional(),
});

export default async (req, res, next) => {
  try {
    const { body, user } = req;
    const data = await schema.validateAsync(body);
    const hashedPassword = await passwordHash(data.password);
    data["password"] = hashedPassword;
    //for other roles than client, add parent id
    if (body.role != "2") {
      data["parent_id"] = user.id;
    }
    //for all roles add createdby
    data["createdBy"] = user.id;
    const result = await createRecord("user", data, {});
    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "user creation failed" });
    }
    //create a new table for client
    if (body.role === "2") {
      const tableName = `form_data_company${result.insertId}`;
      const ddl = `
                  CREATE TABLE IF NOT EXISTS \`${tableName}\` (
                  surrogate_key bigint NOT NULL AUTO_INCREMENT,
                  form_id varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
                  wave_id varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
                  respondent_id varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
                  question_id varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
                  option_numeric varchar(300) COLLATE utf8mb4_general_ci NOT NULL,
                  answer text COLLATE utf8mb4_general_ci DEFAULT NULL,
                  created_at timestamp NULL DEFAULT current_timestamp(),
                  PRIMARY KEY (surrogate_key)
                  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
                `;
      // city varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
      // region varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
      // country varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
      try {
        const conn = await pool.getConnection();
        await conn.query(ddl);
        conn.release();
        console.log(`Table '${tableName}' created for user ${result.insertId}`);
      } catch (ddlErr) {
        console.error("Error creating user-specific table:", ddlErr);
        return res
          .status(500)
          .json({ message: "User created but table creation failed" });
      }
    }
    return res.json({
      message: "User Registered Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    console.log("Error adding user", error);
    if (error.code === "ER_DUP_ENTRY") {
      return res.status(404).json({ message: "Email already exists" });
      //return res.status(409).json({ message: "emailAlreadyExists" });
    }
    next(error);
  }
};
