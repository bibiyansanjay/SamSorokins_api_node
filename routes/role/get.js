import pool from "../../db";
import { parseQueryParams } from "../../utils/parseQueryParams";

export default async (req, res, next) => {
  try {
    let { whereConditions, options } = parseQueryParams(req.originalUrl);
    const userRole = req.user.role;
    const userId = req.user.id;
    const parentId = req.user.parent_id;

    const whereClauses = [];
    const values = [];

    // Super Admin Developer – exclude own role
    if (userRole == 6) {
      //whereClauses.push("(u.`name` NOT IN ('super_admin_developer')");
      whereClauses.push("(u.`company` IS NULL)");
    }

    // Admin – exclude own + super_admin_developer
    if (userRole == 1) {
      whereClauses.push(
        "(u.`name` NOT IN ('admin', 'super_admin_developer') AND u.`company` IS NULL)"
      );
    }
    // Client – complex filter: exclude roles + include those created by self
    if (userRole != "1" && userRole != "6") {
      whereClauses.push(
        "(u.`name` NOT IN ('client', 'admin', 'super_admin_developer') OR u.`company` = ?)"
      );
      values.push(parentId ? parentId : userId);
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
      u.name,
      u.isActive,
      u.isDeleted,
      -- DATE_FORMAT(u.deleted_at, '%d %M %Y') AS deleted_at,
      u.deleted_at,
      u.created_by,
      -- DATE_FORMAT(u.created_at, '%d %M %Y') AS created_at,
      u.created_at,
      u.company,
      CONCAT(cb.firstName, ' ', cb.lastName) AS created_by_name,
      CONCAT(cmp.firstName, ' ', cmp.lastName) AS company_name
    FROM \`role\` u
    LEFT JOIN \`user\` cb ON u.created_by = cb.id
    LEFT JOIN \`user\` cmp ON u.company = cmp.id
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
    console.log("Get role RECORD--------------------------------------");
    console.log("🔍 SQL with JOINs:\n", debugSql);
    console.log("Get role RECORD--------------------------------------");

    // Count total roles (without pagination)
    const countSql = `
      SELECT COUNT(*) AS total FROM \`role\` u
      LEFT JOIN \`user\` cb ON u.created_by = cb.id
      LEFT JOIN \`user\` cmp ON u.company = cmp.id
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
          : "Role data fetched successfully",
      roles: rowsResult,
      userCount: countResult[0]?.total || 0,
    });
  } catch (error) {
    console.error("GET /roles - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching roles",
      error: error.message,
    });
  }
};
