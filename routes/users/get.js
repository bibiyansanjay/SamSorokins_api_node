import pool from "../../db";
import { parseQueryParams } from "../../utils/parseQueryParams";

export default async (req, res) => {
  try {
    const { whereConditions, options } = parseQueryParams(req.originalUrl);
    const userId = req.user.id;
    const userRole = req.user.role;
    //console.log("userId:", userId);
    //console.log("userRole:", userRole);
    const whereClauses = [];
    const values = [];

    // Apply parent filter if not admin or super admin
    // if ((userRole != "1") & (userRole != "6")) {
    //   //console.log("Applying parent filter for user role:", userRole);
    //   whereClauses.push("u.parent_id = ?");
    //   values.push(userRole);
    //   // Exclude self from results
    //   whereClauses.push("u.id != ?");
    //   values.push(userId);
    // }

    // Apply parent filter if not admin or super admin
    if (userRole != "1" && userRole != "6") {
      whereClauses.push("u.parent_id = ?");
      values.push(userRole);

      // Exclude self from results
      whereClauses.push("u.id != ?");
      values.push(userId);
    }

    // Special condition: If user is role 1 (admin), hide users with role 6
    if (userRole == "1") {
      whereClauses.push("u.role != ?");
      values.push("6");
    }

    // Apply advanced filters
    for (const [field, condition] of Object.entries(whereConditions)) {
      if (field === "$or" && Array.isArray(condition)) {
        const orClauses = condition.map((item) => {
          const [key, val] = Object.entries(item)[0];
          if (["IN", "NOT IN"].includes(val.operator.toUpperCase())) {
            const placeholders = val.value.map(() => "?").join(", ");
            values.push(...val.value);
            return `u.\`${key}\` ${val.operator.toUpperCase()} (${placeholders})`;
          } else {
            values.push(val.value);
            return `u.\`${key}\` ${val.operator} ?`;
          }
        });
        whereClauses.push(`(${orClauses.join(" OR ")})`);
      } else if (typeof condition === "object" && condition.operator) {
        const operator = condition.operator.toUpperCase();
        if (["IN", "NOT IN"].includes(operator)) {
          const placeholders = condition.value.map(() => "?").join(", ");
          whereClauses.push(`u.\`${field}\` ${operator} (${placeholders})`);
          values.push(...condition.value);
        } else {
          whereClauses.push(`u.\`${field}\` ${operator} ?`);
          values.push(condition.value);
        }
      } else {
        whereClauses.push(`u.\`${field}\` = ?`);
        values.push(condition);
      }
    }

    const whereClause =
      whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

    // Sorting: use requested field or fallback to created_at DESC
    let orderClause = "";
    if (options.sort?.field) {
      const dir = options.sort.order === "DESC" ? "DESC" : "ASC";
      orderClause = `ORDER BY u.\`${options.sort.field}\` ${dir}`;
    } else {
      orderClause = `ORDER BY u.created_at DESC`;
    }

    // Pagination
    let paginationClause = "";
    if (options.limit !== undefined && options.offset !== undefined) {
      paginationClause = `LIMIT ? OFFSET ?`;
      values.push(options.limit, options.offset);
    } else if (options.limit !== undefined) {
      paginationClause = `LIMIT ?`;
      values.push(options.limit);
    }

    const sql = `
    SELECT 
      u.id,
      u.firstName,
      u.lastName,
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
      r.name AS role_name,
      CONCAT(cu.firstName, ' ', cu.lastName) AS parent_name
    FROM \`user\` u
    LEFT JOIN \`role\` r ON u.role = r.id
    LEFT JOIN \`user\` cu ON u.parent_id = cu.id
    ${whereClause}
    ${orderClause}
    ${paginationClause}
  `;

    // Debug full query
    let debugSql = sql;
    for (const val of values) {
      const escaped = typeof val === "string" ? `'${val}'` : val;
      debugSql = debugSql.replace("?", escaped);
    }
    //console.log("🔍 SQL with JOINs:\n", debugSql);

    // Count total users (without pagination)
    const countSql = `
      SELECT COUNT(*) AS total FROM \`user\` u
      LEFT JOIN \`role\` r ON u.role = r.id
      LEFT JOIN \`user\` cu ON u.parent_id = cu.id
      ${whereClause}
    `;
    // Fire both queries at once (uses 2 pool connections)
    const [[rowsResult], [countResult]] = await Promise.all([
      pool.query(sql, values),
      pool.query(
        countSql,
        values.slice(0, values.length - (options.limit ? 2 : 0))
      ),
    ]);

    return res.status(200).json({
      success: true,
      message:
        rowsResult.length === 0
          ? "No records found"
          : "Users fetched Successfully",
      users: rowsResult,
      userCount: countResult[0]?.total || 0,
    });
  } catch (error) {
    console.error("GET /users - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching users",
      error: error.message,
    });
  }
};
