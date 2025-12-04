// services/ragTrend.service.js
import pool from "../db";

//((((((((((((((((((((((((((((((((((((((helpers starts))))))))))))))))))))))))))))))))))))))
function pickVariant({ company_id, category_id, focus_area }) {
  if (company_id && !focus_area && !category_id)
    return {
      suffix: "packbuilder",
      params: [company_id],
    };
  if (category_id && company_id && !focus_area)
    return {
      suffix: "packbuilder_category",
      params: [company_id, category_id],
    };
  if (focus_area && !category_id && company_id)
    return {
      suffix: "packbuilder_focus_area",
      params: [company_id, focus_area],
    };
  if (category_id && focus_area && company_id)
    return {
      suffix: "packbuilder_category_focus_area",
      params: [company_id, category_id, focus_area],
    };
  throw new Error("Unsupported filter combination");
}

//((((((((((((((((((((((((((((((((((((((helpers ends))))))))))))))))))))))))))))))))))))))

/** 2-B) Rag Score */
export async function getAllSubcategories(filters) {
  const { suffix, params } = pickVariant(filters);
  if (suffix == "packbuilder") {
    const company_id = params[0];
    /*     const sql1 = `SELECT
                    sub.id as value,
                    sub.name as label,
                    CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                    sub.image,
                    sub.image_name,
                    sub.frequency,
                    sub.primary_objective,
                    sub.category_id,
                    cat.name as category_name,
                        -- Combined total
                        (
                            CASE 
                                WHEN sub.core_stakeholder IS NULL OR sub.core_stakeholder = '' THEN 0
                                ELSE LENGTH(sub.core_stakeholder) - LENGTH(REPLACE(sub.core_stakeholder, ',', '')) + 1
                            END
                            +
                            CASE 
                                WHEN sub.complimentary_stakeholder IS NULL OR sub.complimentary_stakeholder = '' THEN 0
                                ELSE LENGTH(sub.complimentary_stakeholder) - LENGTH(REPLACE(sub.complimentary_stakeholder, ',', '')) + 1
                            END
                        ) AS total_stakeholder_count
                  FROM subcategory sub
                  LEFT JOIN category cat ON sub.category_id = cat.id
                  WHERE sub.isActive = 1 AND sub.isDeleted = 0 AND (sub.company IS NULL OR sub.company = ?)
                      AND sub.id NOT IN (
                            SELECT subcategory_id 
                            FROM user_subcategory_form usf
                            WHERE usf.subcategory_id = sub.id
                              AND usf.company = ? AND usf.isDeleted = 0
                        )
                      -- only those subcategories which have questions
                      AND sub.id IN (
                          SELECT subcategory_id
                          FROM question q
                          WHERE q.subcategory_id = sub.id
                            AND (q.company = ? OR q.company IS NULL)
                      )
                            ORDER BY sub.name ASC;`;
    console.log(sql1);
    const [rows1] = await pool.query(sql1, [
      company_id,
      company_id,
      company_id,
    ]);
    console.log(rows1);
    return rows1; */

    const sql = `SELECT
                      sub.id as value,
                      sub.name as label,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.image,
                      sub.image_name,
                      sub.frequency,
                      sub.primary_objective,
                      sub.category_id,
                      cat.name as category_name,
                      -- Combined total stakeholder count (unchanged)
                      (
                          CASE 
                              WHEN sub.core_stakeholder IS NULL OR sub.core_stakeholder = '' THEN 0
                              ELSE LENGTH(sub.core_stakeholder) - LENGTH(REPLACE(sub.core_stakeholder, ',', '')) + 1
                          END
                          +
                          CASE 
                              WHEN sub.complimentary_stakeholder IS NULL OR sub.complimentary_stakeholder = '' THEN 0
                              ELSE LENGTH(sub.complimentary_stakeholder) - LENGTH(REPLACE(sub.complimentary_stakeholder, ',', '')) + 1
                          END
                      ) AS total_stakeholder_count,
                      
                      -- New: Check if questions exist for this subcategory
                      CASE
                          WHEN COUNT(q.subcategory_id) > 0 THEN 'yes'
                          ELSE 'no'
                      END AS isQuestion
                  FROM 
                      subcategory sub
                  LEFT JOIN 
                      category cat ON sub.category_id = cat.id
                  -- LEFT JOIN on questions to check for existence
                  LEFT JOIN 
                      question q ON sub.id = q.subcategory_id AND (q.company = ? OR q.company IS NULL)
                  WHERE 
                      sub.isActive = 1 
                      AND sub.isDeleted = 0 
                      AND (sub.company IS NULL OR sub.company = ?) 
                      AND sub.id NOT IN (
                          SELECT subcategory_id 
                          FROM user_subcategory_form usf
                          WHERE usf.subcategory_id = sub.id
                            AND usf.company = ? AND usf.isDeleted = 0
                      )
                  GROUP BY 
                      sub.id, sub.name, cat.abbreviation, sub.image, sub.image_name, 
                      sub.frequency, sub.primary_objective, sub.category_id, cat.name, 
                      sub.core_stakeholder, sub.complimentary_stakeholder
                  ORDER BY 
                      sub.name ASC`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id, company_id, company_id]);
    return rows;
  } else if (suffix == "packbuilder_category") {
    const company_id = params[0];
    const category_id = params[1];
    /*     const sql = `SELECT
                    sub.id as value,
                    sub.name as label,
                    CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                    sub.image,
                    sub.image_name,
                    sub.frequency,
                    sub.primary_objective,
                    sub.category_id,
                    cat.name as category_name,
                        -- Combined total
                        (
                            CASE 
                                WHEN sub.core_stakeholder IS NULL OR sub.core_stakeholder = '' THEN 0
                                ELSE LENGTH(sub.core_stakeholder) - LENGTH(REPLACE(sub.core_stakeholder, ',', '')) + 1
                            END
                            +
                            CASE 
                                WHEN sub.complimentary_stakeholder IS NULL OR sub.complimentary_stakeholder = '' THEN 0
                                ELSE LENGTH(sub.complimentary_stakeholder) - LENGTH(REPLACE(sub.complimentary_stakeholder, ',', '')) + 1
                            END
                        ) AS total_stakeholder_count
                  FROM subcategory sub
                  LEFT JOIN category cat ON sub.category_id = cat.id
                  WHERE sub.isActive = 1 AND sub.isDeleted = 0 AND (sub.company IS NULL OR sub.company = ?) AND sub.category_id = ?
                      AND sub.id NOT IN (
                            SELECT subcategory_id 
                            FROM user_subcategory_form usf
                            WHERE usf.subcategory_id = sub.id
                              AND usf.company = ? AND usf.isDeleted = 0
                        )
                      -- only those subcategories which have questions
                      AND sub.id IN (
                          SELECT subcategory_id
                          FROM question q
                          WHERE q.subcategory_id = sub.id
                            AND (q.company = ? OR q.company IS NULL)
                      )
                            ORDER BY sub.name ASC`;
    console.log(sql); */

    const sql = `SELECT
                      sub.id as value,
                      sub.name as label,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.image,
                      sub.image_name,
                      sub.frequency,
                      sub.primary_objective,
                      sub.category_id,
                      cat.name as category_name,
                      -- Combined total stakeholder count (unchanged)
                      (
                          CASE 
                              WHEN sub.core_stakeholder IS NULL OR sub.core_stakeholder = '' THEN 0
                              ELSE LENGTH(sub.core_stakeholder) - LENGTH(REPLACE(sub.core_stakeholder, ',', '')) + 1
                          END
                          +
                          CASE 
                              WHEN sub.complimentary_stakeholder IS NULL OR sub.complimentary_stakeholder = '' THEN 0
                              ELSE LENGTH(sub.complimentary_stakeholder) - LENGTH(REPLACE(sub.complimentary_stakeholder, ',', '')) + 1
                          END
                      ) AS total_stakeholder_count,
                      
                      -- New: Check if questions exist for this subcategory
                      CASE
                          WHEN COUNT(q.subcategory_id) > 0 THEN 'yes'
                          ELSE 'no'
                      END AS isQuestion
                  FROM 
                      subcategory sub
                  LEFT JOIN 
                      category cat ON sub.category_id = cat.id
                  -- LEFT JOIN on questions to check for existence
                  LEFT JOIN 
                      question q ON sub.id = q.subcategory_id AND (q.company = ? OR q.company IS NULL)
                  WHERE 
                      sub.isActive = 1 
                      AND sub.isDeleted = 0 
                      AND (sub.company IS NULL OR sub.company = ?) 
                      AND sub.category_id = ?
                      AND sub.id NOT IN (
                          SELECT subcategory_id 
                          FROM user_subcategory_form usf
                          WHERE usf.subcategory_id = sub.id
                            AND usf.company = ? AND usf.isDeleted = 0
                      )
                  GROUP BY 
                      sub.id, sub.name, cat.abbreviation, sub.image, sub.image_name, 
                      sub.frequency, sub.primary_objective, sub.category_id, cat.name, 
                      sub.core_stakeholder, sub.complimentary_stakeholder
                  ORDER BY 
                      sub.name ASC`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      company_id,
      company_id,
      category_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "packbuilder_focus_area") {
    const company_id = params[0];
    const focus_area = params[1];
    /*     const sql = `SELECT
                    sub.id as value,
                    sub.name as label,
                    CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                    sub.image,
                    sub.image_name,
                    sub.frequency,
                    sub.primary_objective,
                    sub.category_id,
                    cat.name as category_name,
                        -- Combined total
                        (
                            CASE 
                                WHEN sub.core_stakeholder IS NULL OR sub.core_stakeholder = '' THEN 0
                                ELSE LENGTH(sub.core_stakeholder) - LENGTH(REPLACE(sub.core_stakeholder, ',', '')) + 1
                            END
                            +
                            CASE 
                                WHEN sub.complimentary_stakeholder IS NULL OR sub.complimentary_stakeholder = '' THEN 0
                                ELSE LENGTH(sub.complimentary_stakeholder) - LENGTH(REPLACE(sub.complimentary_stakeholder, ',', '')) + 1
                            END
                        ) AS total_stakeholder_count
                  FROM subcategory sub
                  LEFT JOIN category cat ON sub.category_id = cat.id
                  WHERE sub.isActive = 1 AND sub.isDeleted = 0 AND (sub.company IS NULL OR sub.company = ?) AND sub.focus_area = ?
                      AND sub.id NOT IN (
                            SELECT subcategory_id 
                            FROM user_subcategory_form usf
                            WHERE usf.subcategory_id = sub.id
                              AND usf.company = ? AND usf.isDeleted = 0
                        )
                      -- only those subcategories which have questions
                      AND sub.id IN (
                          SELECT subcategory_id
                          FROM question q
                          WHERE q.subcategory_id = sub.id
                            AND (q.company = ? OR q.company IS NULL)
                      )
                            ORDER BY sub.name ASC;`;
    console.log(sql); */

    const sql = `SELECT
                      sub.id as value,
                      sub.name as label,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.image,
                      sub.image_name,
                      sub.frequency,
                      sub.primary_objective,
                      sub.category_id,
                      cat.name as category_name,
                      -- Combined total stakeholder count (unchanged)
                      (
                          CASE 
                              WHEN sub.core_stakeholder IS NULL OR sub.core_stakeholder = '' THEN 0
                              ELSE LENGTH(sub.core_stakeholder) - LENGTH(REPLACE(sub.core_stakeholder, ',', '')) + 1
                          END
                          +
                          CASE 
                              WHEN sub.complimentary_stakeholder IS NULL OR sub.complimentary_stakeholder = '' THEN 0
                              ELSE LENGTH(sub.complimentary_stakeholder) - LENGTH(REPLACE(sub.complimentary_stakeholder, ',', '')) + 1
                          END
                      ) AS total_stakeholder_count,
                      
                      -- New: Check if questions exist for this subcategory
                      CASE
                          WHEN COUNT(q.subcategory_id) > 0 THEN 'yes'
                          ELSE 'no'
                      END AS isQuestion
                  FROM 
                      subcategory sub
                  LEFT JOIN 
                      category cat ON sub.category_id = cat.id
                  -- LEFT JOIN on questions to check for existence
                  LEFT JOIN 
                      question q ON sub.id = q.subcategory_id AND (q.company = ? OR q.company IS NULL)
                  WHERE 
                      sub.isActive = 1 
                      AND sub.isDeleted = 0 
                      AND (sub.company IS NULL OR sub.company = ?) 
                      AND sub.focus_area = ?
                      AND sub.id NOT IN (
                          SELECT subcategory_id 
                          FROM user_subcategory_form usf
                          WHERE usf.subcategory_id = sub.id
                            AND usf.company = ? AND usf.isDeleted = 0
                      )
                  GROUP BY 
                      sub.id, sub.name, cat.abbreviation, sub.image, sub.image_name, 
                      sub.frequency, sub.primary_objective, sub.category_id, cat.name, 
                      sub.core_stakeholder, sub.complimentary_stakeholder
                  ORDER BY 
                      sub.name ASC`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      company_id,
      company_id,
      focus_area,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "packbuilder_category_focus_area") {
    const company_id = params[0];
    const category_id = params[1];
    const focus_area = params[2];
    /*     const sql = `SELECT
                    sub.id as value,
                    sub.name as label,
                    CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                    sub.image,
                    sub.image_name,
                    sub.frequency,
                    sub.primary_objective,
                    sub.category_id,
                    cat.name as category_name,
                        -- Combined total
                        (
                            CASE 
                                WHEN sub.core_stakeholder IS NULL OR sub.core_stakeholder = '' THEN 0
                                ELSE LENGTH(sub.core_stakeholder) - LENGTH(REPLACE(sub.core_stakeholder, ',', '')) + 1
                            END
                            +
                            CASE 
                                WHEN sub.complimentary_stakeholder IS NULL OR sub.complimentary_stakeholder = '' THEN 0
                                ELSE LENGTH(sub.complimentary_stakeholder) - LENGTH(REPLACE(sub.complimentary_stakeholder, ',', '')) + 1
                            END
                        ) AS total_stakeholder_count
                  FROM subcategory sub
                  LEFT JOIN category cat ON sub.category_id = cat.id
                  WHERE sub.isActive = 1 AND sub.isDeleted = 0 AND (sub.company IS NULL OR sub.company = ?) AND sub.category_id = ? AND sub.focus_area = ?
                      AND sub.id NOT IN (
                            SELECT subcategory_id 
                            FROM user_subcategory_form usf
                            WHERE usf.subcategory_id = sub.id
                              AND usf.company = ? AND usf.isDeleted = 0
                        )
                      -- only those subcategories which have questions
                      AND sub.id IN (
                          SELECT subcategory_id
                          FROM question q
                          WHERE q.subcategory_id = sub.id
                            AND (q.company = ? OR q.company IS NULL)
                      )
                            ORDER BY sub.name ASC;`; */

    const sql = `SELECT
                      sub.id as value,
                      sub.name as label,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.image,
                      sub.image_name,
                      sub.frequency,
                      sub.primary_objective,
                      sub.category_id,
                      cat.name as category_name,
                      -- Combined total stakeholder count (unchanged)
                      (
                          CASE 
                              WHEN sub.core_stakeholder IS NULL OR sub.core_stakeholder = '' THEN 0
                              ELSE LENGTH(sub.core_stakeholder) - LENGTH(REPLACE(sub.core_stakeholder, ',', '')) + 1
                          END
                          +
                          CASE 
                              WHEN sub.complimentary_stakeholder IS NULL OR sub.complimentary_stakeholder = '' THEN 0
                              ELSE LENGTH(sub.complimentary_stakeholder) - LENGTH(REPLACE(sub.complimentary_stakeholder, ',', '')) + 1
                          END
                      ) AS total_stakeholder_count,
                      
                      -- New: Check if questions exist for this subcategory
                      CASE
                          WHEN COUNT(q.subcategory_id) > 0 THEN 'yes'
                          ELSE 'no'
                      END AS isQuestion
                  FROM 
                      subcategory sub
                  LEFT JOIN 
                      category cat ON sub.category_id = cat.id
                  -- LEFT JOIN on questions to check for existence
                  LEFT JOIN 
                      question q ON sub.id = q.subcategory_id AND (q.company = ? OR q.company IS NULL)
                  WHERE 
                      sub.isActive = 1 
                      AND sub.isDeleted = 0 
                      AND (sub.company IS NULL OR sub.company = ?) 
                      AND sub.focus_area = ?
                      AND sub.category_id = ?
                      AND sub.id NOT IN (
                          SELECT subcategory_id 
                          FROM user_subcategory_form usf
                          WHERE usf.subcategory_id = sub.id
                            AND usf.company = ? AND usf.isDeleted = 0
                      )
                  GROUP BY 
                      sub.id, sub.name, cat.abbreviation, sub.image, sub.image_name, 
                      sub.frequency, sub.primary_objective, sub.category_id, cat.name, 
                      sub.core_stakeholder, sub.complimentary_stakeholder
                  ORDER BY 
                      sub.name ASC`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      company_id,
      company_id,
      focus_area,
      category_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 1) Trend (avg delta for latest/selected wave) */
export async function getAllDomains(filters) {
  const { suffix, params } = pickVariant(filters);
  const company_id = params[0];
  const sql = `SELECT 
            DISTINCT(focus_area) as label,
            focus_area as value 
          FROM \`subcategory\` 
          WHERE isActive = 1 AND isDeleted = 0 AND focus_area IS NOT NULL AND (company = ? OR company IS NULL)
          ORDER BY focus_area ASC`;
  // console.log(sql);
  const [rows] = await pool.query(sql, company_id);
  // console.log("Fetched categorys:", rows);
  return rows;
}

/** 2-A) getAllCategories with filters */
export async function getAllCategories(filters) {
  const { suffix, params } = pickVariant(filters);
  const company_id = params[0];
  const sql = `
      SELECT 
        DISTINCT sub.category_id AS value,
        cat.name AS label        
      FROM \`subcategory\` sub
      LEFT JOIN category cat ON sub.category_id = cat.id
      WHERE sub.isActive = 1 AND sub.isDeleted = 0 AND (sub.company IS NULL OR sub.company = ?) AND cat.isActive = 1 AND cat.isDeleted = 0
      ORDER BY cat.name ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [company_id]);
  //console.log(rows);
  return rows;
  //
}
