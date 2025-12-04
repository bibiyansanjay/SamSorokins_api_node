import pool from "../../db";

export default async (req, res, next) => {
  try {
    const sql = `
      SELECT 
        cat.id as value,
        CONCAT(cat.firstName, ' ', cat.lastName) AS label   
      FROM \`user\` cat
      WHERE cat.isDeleted = 0 AND cat.isActive = 1 AND cat.role = 2
      ORDER BY cat.firstName ASC
    `;

    const [rows] = await pool.query(sql);
    //console.log("Fetched users:", rows);

    return res.status(200).json({
      success: true,
      message: "Users data fetched successfully",
      data: rows,
    });
  } catch (error) {
    console.error("GET /users clients - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching Clients",
      error: error.message,
    });
  }
};
