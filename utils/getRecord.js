import pool from "../db";

const getRecord = (tableName, whereConditions = {}, options = {}) => {
  return new Promise(async (resolve, reject) => {
    try {
      if (!tableName) throw new Error("Table name is required");

      const selectFields =
        options.selectFields?.length > 0
          ? options.selectFields.map((f) => `\`${f}\``).join(", ")
          : "*";

      let whereClause = "";
      const whereValues = [];
      const whereClauses = [];

      for (const [field, condition] of Object.entries(whereConditions)) {
        if (field === "$or" && Array.isArray(condition)) {
          const orClauses = condition.map((item) => {
            const [key, val] = Object.entries(item)[0];
            if (["IN", "NOT IN"].includes(val.operator.toUpperCase())) {
              const placeholders = val.value.map(() => "?").join(", ");
              whereValues.push(...val.value);
              return `\`${key}\` ${val.operator.toUpperCase()} (${placeholders})`;
            } else {
              whereValues.push(val.value);
              return `\`${key}\` ${val.operator} ?`;
            }
          });
          whereClauses.push(`(${orClauses.join(" OR ")})`);
        } else if (typeof condition === "object" && condition.operator) {
          const operator = condition.operator.toUpperCase();
          if (["IN", "NOT IN"].includes(operator)) {
            const placeholders = condition.value.map(() => "?").join(", ");
            whereClauses.push(`\`${field}\` ${operator} (${placeholders})`);
            whereValues.push(...condition.value);
          } else {
            whereClauses.push(`\`${field}\` ${operator} ?`);
            whereValues.push(condition.value);
          }
        } else {
          whereClauses.push(`\`${field}\` = ?`);
          whereValues.push(condition);
        }
      }

      if (whereClauses.length > 0) {
        whereClause = `WHERE ${whereClauses.join(" AND ")}`;
      }

      let orderByClause = "";
      if (options.sort?.field) {
        const dir = options.sort.order === "DESC" ? "DESC" : "ASC";
        orderByClause = `ORDER BY \`${options.sort.field}\` ${dir}`;
      }

      let limitOffsetClause = "";
      if (options.limit !== undefined && options.offset !== undefined) {
        limitOffsetClause = "LIMIT ? OFFSET ?";
        whereValues.push(options.limit, options.offset);
      } else if (options.limit !== undefined) {
        limitOffsetClause = "LIMIT ?";
        whereValues.push(options.limit);
      }

      const sql = `
        SELECT ${selectFields} FROM \`${tableName}\`
        ${whereClause}
        ${orderByClause}
        ${limitOffsetClause}
      `;

      // Debug SQL
      let debugSql = sql;
      for (const val of whereValues) {
        const escaped = typeof val === "string" ? `'${val}'` : val;
        debugSql = debugSql.replace("?", escaped);
      }
      console.log("get RECORD--------------------------------------");
      console.log("🔍 Final SQL:\n", debugSql);
      console.log("get RECORD--------------------------------------");

      const [rows] = await pool.query(sql, whereValues);
      resolve(rows);
    } catch (err) {
      reject(err);
    }
  });
};

export default getRecord;

// Example usage of getRecord function
//----------/kpi?name[like]=Board Engagement&created_at[between]=2024-01-01,2025-12-31&isActive=1&limit=10&offset=0&sortField=created_at&sortOrder=desc
// ✅ Supported Use Cases (Examples)
// Purpose	Example Query Param
// Equal	?role=2
// Not Equal	?status[ne]=inactive
// Greater Than	?score[gt]=80
// Less Than	?score[lt]=50
// Between Dates	?created_at[between]=2024-01-01,2024-12-31
// Like Search	?name[like]=John
// Multi-field Search	?searchFields=name,email&searchValue=john
// Pagination	?paginationModel[page]=1&paginationModel[pageSize]=10
// Sorting	?sortModel[0][field]=name&sortModel[0][sort]=asc
// Boolean toggle	?showDeletedUsers=true
