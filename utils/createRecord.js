import pool from "../db";

const createRecord = (tableName, fieldsToUpdate = {}) => {
  return new Promise(async (resolve, reject) => {
    try {
      if (!tableName) {
        throw new Error("Table name is required");
      }

      if (Object.keys(fieldsToUpdate).length === 0) {
        throw new Error("No fields to update");
      }
      // CREATE (INSERT)
      const insertFields = Object.keys(fieldsToUpdate);
      const insertValues = Object.values(fieldsToUpdate);
      const placeholders = insertFields.map(() => "?").join(", ");

      const sql = `
          INSERT INTO \`${tableName}\` (${insertFields.join(", ")})
          VALUES (${placeholders})
        `;
      console.log("sql", sql);
      const [result] = await pool.query(sql, insertValues);
      console.log("CREATE RECORD--------------------------------------");
      console.log(`Inserted into '${tableName}' (ID: ${result.insertId})`);
      console.log("CREATE RECORD--------------------------------------");

      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
};

export default createRecord;
// Example usage of createRecord function
// await createRecord(
//   'user',
//   { otp: '123456', lastActive: new Date() }
// );
