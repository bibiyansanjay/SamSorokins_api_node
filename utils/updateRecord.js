import pool from "../db";

const updateRecord = (tableName, fieldsToUpdate = {}, whereConditions = {}) => {
  return new Promise(async (resolve, reject) => {
    try {
      if (!tableName) {
        throw new Error("Table name is required");
      }

      if (Object.keys(fieldsToUpdate).length === 0) {
        throw new Error("No fields to update");
      }

      if (Object.keys(whereConditions).length === 0) {
        throw new Error("Where conditions are required");
      }

      // Build SET clause
      const setClauses = Object.keys(fieldsToUpdate).map(
        (field) => `${field} = ?`
      );
      const setValues = Object.values(fieldsToUpdate);

      // Build WHERE clause
      const whereClauses = Object.keys(whereConditions).map(
        (field) => `${field} = ?`
      );
      const whereValues = Object.values(whereConditions);

      const sql = `
        UPDATE \`${tableName}\`
        SET ${setClauses.join(", ")}
        WHERE ${whereClauses.join(" AND ")}
      `;

      // Debug log
      console.log("UPDATE RECORD--------------------------------------");
      console.log("Running SQL:", sql);
      console.log("With values:", [...setValues, ...whereValues]);
      console.log("UPDATE RECORD--------------------------------------");
      // Execute query
      const [result] = await pool.query(sql, [...setValues, ...whereValues]);

      console.log(`Table '${tableName}' updated (${result.affectedRows} rows)`);

      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
};

export default updateRecord;
// Example usage of updateRecord function
// await updateTable(
//   'user',
//   { otp: '123456', lastActive: new Date() },
//   { email: 'test@example.com', is_deleted: 0 }
// );
// await updateTable(
//   'orders',
//   { status: 'SHIPPED', shippedAt: new Date() },
//   { order_id: 1001 }
// );
