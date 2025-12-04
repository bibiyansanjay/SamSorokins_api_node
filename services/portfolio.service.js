// services/formView.service.js
import pool from "../db";

//((((((((((((((((((((((((((((((((((((((helpers starts))))))))))))))))))))))))))))))))))))))
function pickVariant({
  wave_id,
  subcategory,
  company_id,
  city,
  region,
  country,
}) {
  // if (!!wave_id && !subcategory)
  //   return { suffix: "none", params: [] };
  if (!wave_id && company_id)
    return {
      suffix: "portfolio",
      params: [company_id, city, region, country],
    };
  if (wave_id && company_id)
    return {
      suffix: "portfolio_wave",
      params: [wave_id, company_id, city, region, country],
    };
  // if (!wave_id && company_id)
  //   return {
  //     suffix: "portfolio_segment",
  //     params: [subcategory, company_id, city, region, country],
  //   };
  // if (wave_id && company_id)
  //   return {
  //     suffix: "portfolio_wave_segment",
  //     params: [wave_id, subcategory, company_id, city, region, country],
  //   };
  throw new Error("Unsupported filter combination");
}

function applyExtraFilters(baseSql, filters) {
  let whereParts = [];
  //console.log("Filters in applyExtraFilters:", filters);
  if (
    filters.city &&
    typeof filters.city === "string" &&
    filters.city.length > 0
  ) {
    //console.log("Applying city filter with value:", filters.city);
    whereParts.push(`cu.city = '${filters.city}'`);
  }
  if (
    filters.region &&
    typeof filters.region === "string" &&
    filters.region.length > 0
  ) {
    whereParts.push(`cu.region = '${filters.region}'`);
  }

  if (
    filters.country &&
    typeof filters.country === "string" &&
    filters.country.length > 0
  ) {
    whereParts.push(`cu.country = '${filters.country}'`);
  }

  if (
    filters.dateFilter &&
    typeof filters.dateFilter === "string" &&
    filters.dateFilter.length > 0
  ) {
    whereParts.push(`fdc.created_at < ${filters.dateFilter}`);
  }

  // Inject extra WHERE parts before GROUP BY
  let sql = baseSql;
  if (whereParts.length > 0) {
    //console.log("Applying extra WHERE conditions:", whereParts);
    sql = sql.replace("/*EXTRA_FILTERS*/", " AND " + whereParts.join(" AND "));
  } else {
    sql = sql.replace("/*EXTRA_FILTERS*/", "");
  }
  if (sql.includes("/*PACK_FILTERS*/")) {
    if (
      filters.subcategory &&
      typeof filters.subcategory === "number" &&
      filters.subcategory > 0
    ) {
      sql = sql.replace(
        "/*PACK_FILTERS*/",
        `AND usf.subcategory_id = '${filters.subcategory}'`
      );
    } else {
      sql = sql.replace("/*PACK_FILTERS*/", "");
    }
  }
  return { sql };
}

// Utility function to extract distinct categories from rows
async function extractDistinctCategories(rows = []) {
  const uniqueCategories = Array.from(
    new Map(
      rows
        .filter((r) => r.category_id && r.category_name)
        .map((r) => [
          r.category_id,
          {
            category_id: r.category_id,
            label: r.category_name,
            value: r.category_name,
          },
        ])
    ).values()
  );
  uniqueCategories.sort((a, b) => a.value.localeCompare(b.value));
  return uniqueCategories;
}

async function enrichWithStakeholders(row, company_id) {
  if (!row || !row.stakeholder_ids) return row;

  const allIds = row.stakeholder_ids
    .split(",")
    .map((id) => parseInt(id.trim()))
    .filter(Boolean);

  if (allIds.length === 0) return row;

  const sql = `
          SELECT s.id AS stakeholder_id, s.name AS stakeholder_name, ROUND(AVG(fdc.option_numeric), 2) AS rag_score, usf.id AS form_id
          FROM stakeholder s
          INNER JOIN user_stakeholder_form usf ON usf.stakeholder_id = s.id AND usf.subcategory_id = ?
          INNER JOIN form_data_company2 AS fdc ON fdc.form_id = usf.id
          WHERE s.id IN (?) AND usf.company = ${company_id}
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          GROUP BY fdc.form_id
          ORDER BY stakeholder_id ASC;`;

  // console.log("enrichWithStakeholders SQL:", sql, "with IDs:", row.subcategory_id, allIds);

  const [stakeholders] = await pool.query(sql, [row.subcategory_id, allIds]);
  // console.log("stakeholderMap:", stakeholders);
  row.stakeholders = stakeholders;
  //console.log("enriched row:", row);
  return row;
}
//((((((((((((((((((((((((((((((((((((((helpers ends))))))))))))))))))))))))))))))))))))))

export async function getAllPacksFilter(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    const company_id = params[0];
    const baseSql = `SELECT
                      usf.subcategory_id AS value,
                      sub.name AS label
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY usf.subcategory_id
                    ORDER BY sub.name ASC
                    `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id]);
    //console.log(rows);
    // console.log(enrichedRows);
    // console.log(uniqueCategories);
    // Use results
    return rows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `SELECT
                      usf.subcategory_id AS value,
                      sub.name AS label
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    AND fdc.wave_id = ?
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY sub.name ASC
                    `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id, wave_id]);
    //console.log(rows);
    return rows;
  }
  // else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   const baseSql = `SELECT
  //                     usf.subcategory_id AS value,
  //                     sub.name AS label
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                   `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, company_id]);
  //   //console.log(rows);
  //   // Process data concurrently

  //   return rows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `SELECT
  //                     usf.subcategory_id AS value,
  //                     sub.name AS label
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   and fdc.wave_id = ?
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                   `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [
  //     segment_id,
  //     company_id,
  //     company_id,
  //     wave_id,
  //   ]);
  //   //console.log(rows);
  //   return rows;
  // }
}

/** 2) getAllWaves */
export async function getAllWaves(company_id) {
  const sql = `SELECT DISTINCT CONCAT("Wave ",wave_id)as label, wave_id as value FROM \`wave_question_avg\` where company_id = ? ORDER BY wave_id ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [company_id]);
  return rows;
}

/** 3) getActivePacks */
export async function getActivePacks(company_id) {
  const sql = `SELECT 
                    COUNT(id) AS active_packs
                  FROM user_stakeholder_form
                  WHERE company = ?
            `;
  //console.log(sql);
  const [rows] = await pool.query(sql, [company_id]);
  //console.log(rows);
  return rows?.[0]?.active_packs || 0;
}

/** 4) Rag Score */
export async function getRagScore(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];

  if (suffix == "portfolio") {
    const company_id = params[0];
    baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
        WHERE usf.company = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
          /*PACK_FILTERS*/
  `;
    //console.log(baseSql);
    queryParams = [company_id];
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
        WHERE usf.company = ?    
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
          /*EXTRA_FILTERS*/
          /*PACK_FILTERS*/
  `;
    //console.log(baseSql);
    queryParams = [company_id, wave_id];
  }
  // else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   baseSql = `
  //   SELECT
  //         ROUND(AVG(fdc.option_numeric), 2) AS rag_score
  //       FROM form_data_company${company_id} AS fdc
  //       INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //       INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
  //       INNER JOIN client_segment_users AS csu
  //         ON csu.segment_id = ? AND csu.company=?
  //         AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //       WHERE usf.company = ?
  //         AND fdc.option_numeric REGEXP '^[0-9]+$'
  //         /*EXTRA_FILTERS*/
  //         /*PACK_FILTERS*/
  // `;
  //   //console.log(baseSql);
  //   queryParams = [segment_id, company_id, company_id];
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   baseSql = `
  //   SELECT
  //         ROUND(AVG(fdc.option_numeric), 2) AS rag_score
  //       FROM form_data_company${company_id} AS fdc
  //       INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //       INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
  //       INNER JOIN client_segment_users AS csu
  //         ON csu.segment_id = ? AND csu.company=?
  //         AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //       WHERE usf.company = ?
  //         AND fdc.option_numeric REGEXP '^[0-9]+$'
  //         AND fdc.wave_id = ?
  //         /*EXTRA_FILTERS*/
  //         /*PACK_FILTERS*/
  // `;
  //   //console.log(baseSql);
  //   queryParams = [segment_id, company_id, company_id, wave_id];
  // }
  // Apply filters
  const { sql } = applyExtraFilters(baseSql, filters);
  // Current RAG score (latest)
  const [currentRows] = await pool.query(sql, queryParams);
  // console.log("RAG SQL:", sql);
  const currentScore = currentRows?.[0]?.rag_score || 0;
  //console.log("Current RAG Score:", currentScore);

  // RAG score before 14 days
  filters.dateFilter = "DATE_SUB(CURDATE(), INTERVAL 14 DAY)";
  const { sql: oldSql } = applyExtraFilters(baseSql, filters);
  const [oldRows] = await pool.query(oldSql, queryParams);
  //console.log("Old RAG SQL:", oldSql);
  const oldScore = oldRows?.[0]?.rag_score || 0;
  //console.log("Old RAG Score:", oldScore);
  // Delta calculation
  const delta = parseFloat((currentScore - oldScore).toFixed(2));
  const deltaDisplay = `${delta > 0 ? "+" : delta < 0 ? "" : ""}${delta.toFixed(
    2
  )}`;

  // Final response
  return {
    current_rag_score: currentScore,
    previous_rag_score: oldScore,
    //delta_14_days: delta,
    delta_display: deltaDisplay, // e.g. "+0.2" or "-0.3"
    //delta_label: `${deltaDisplay} last 14 days`,
  };
}

/** 5) getCriticalIssues */
export async function getCriticalIssues(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];

  if (suffix == "portfolio") {
    const company_id = params[0];
    baseSql = `
                    SELECT COUNT(*) AS below_threshold_count
                    FROM (
                      SELECT
                        fdc.question_id,
                        ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                      FROM form_data_company${company_id} AS fdc
                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
                      WHERE usf.company = ?  
                        AND fdc.option_numeric REGEXP '^[0-9]+$'
                        /*EXTRA_FILTERS*/
                        /*PACK_FILTERS*/
                      GROUP BY fdc.question_id
                    ) t
                    WHERE t.avg_score < (
                                        SELECT COALESCE(u.critical_threshold, 2.5)
                                        FROM user u
                                        WHERE u.id = ?
                                        )
  `;
    //console.log(baseSql);
    queryParams = [company_id, company_id];
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    baseSql = `
                    SELECT COUNT(*) AS below_threshold_count
                    FROM (
                      SELECT
                        fdc.question_id,
                        ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                      FROM form_data_company${company_id} AS fdc
                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
                      WHERE usf.company = ?  
                        AND fdc.wave_id = ?   -- added wave filter
                        AND fdc.option_numeric REGEXP '^[0-9]+$'
                        /*EXTRA_FILTERS*/
                        /*PACK_FILTERS*/
                      GROUP BY fdc.question_id
                    ) t
                    WHERE t.avg_score < (
                                        SELECT COALESCE(u.critical_threshold, 2.5)
                                        FROM user u
                                        WHERE u.id = ?
                                        )
  `;
    //console.log(baseSql);
    queryParams = [company_id, wave_id, company_id];
  }
  //  else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   baseSql = `
  //                   SELECT COUNT(*) AS below_threshold_count
  //                   FROM (
  //                     SELECT
  //                       fdc.question_id,
  //                       ROUND(AVG(fdc.option_numeric), 2) AS avg_score
  //                     FROM form_data_company${company_id} AS fdc
  //                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                     INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
  //                     INNER JOIN client_segment_users AS csu
  //                       ON csu.segment_id = ? AND csu.company=?
  //                       AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                     WHERE usf.company = ?
  //                       AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                       /*EXTRA_FILTERS*/
  //                       /*PACK_FILTERS*/
  //                     GROUP BY fdc.question_id
  //                   ) t
  //                   WHERE t.avg_score < (
  //                                       SELECT COALESCE(u.critical_threshold, 2.5)
  //                                       FROM user u
  //                                       WHERE u.id = ?
  //                                       )
  // `;
  //   //console.log(baseSql);
  //   queryParams = [segment_id, company_id, company_id, company_id];
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   baseSql = `
  //                   SELECT COUNT(*) AS below_threshold_count
  //                   FROM (
  //                     SELECT
  //                       fdc.question_id,
  //                       ROUND(AVG(fdc.option_numeric), 2) AS avg_score
  //                     FROM form_data_company${company_id} AS fdc
  //                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                     INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
  //                     INNER JOIN client_segment_users AS csu
  //                       ON csu.segment_id = ? AND csu.company=?
  //                       AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                     WHERE usf.company = ?
  //                       AND fdc.wave_id = ?   -- added wave filter
  //                       AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                       /*EXTRA_FILTERS*/
  //                       /*PACK_FILTERS*/
  //                     GROUP BY fdc.question_id
  //                   ) t
  //                   WHERE t.avg_score < (
  //                                       SELECT COALESCE(u.critical_threshold, 2.5)
  //                                       FROM user u
  //                                       WHERE u.id = ?
  //                                       )
  // `;
  //   //console.log(baseSql);
  //   queryParams = [segment_id, company_id, company_id, wave_id, company_id];
  // }
  // Apply filters
  const { sql } = applyExtraFilters(baseSql, filters);
  // Current  score (latest)
  const [currentRows] = await pool.query(sql, queryParams);
  //console.log("currentRows:", currentRows);
  //console.log(" SQL:", sql);
  const currentScore = currentRows?.[0]?.below_threshold_count || 0;
  //console.log("Current  Score:", currentScore);

  //  score before 14 days
  filters.dateFilter = "DATE_SUB(CURDATE(), INTERVAL 14 DAY)";
  const { sql: oldSql } = applyExtraFilters(baseSql, filters);
  const [oldRows] = await pool.query(oldSql, queryParams);
  //console.log("Old  SQL:", oldSql);
  const oldScore = oldRows?.[0]?.below_threshold_count || 0;
  //console.log("Old  Score:", oldScore);
  // Delta calculation
  const delta = parseFloat((currentScore - oldScore).toFixed(2));
  const deltaDisplay = `${delta > 0 ? "+" : delta < 0 ? "" : ""}${delta.toFixed(
    2
  )}`;

  // Final response
  return {
    current_below_threshold_count: currentScore,
    previous_below_threshold_count: oldScore,
    //delta_14_days: delta,
    delta_display: deltaDisplay, // e.g. "+0.2" or "-0.3"
    //delta_label: `${deltaDisplay} last 14 days`,
  };
}

/** 6) Get date range according to form n filters */
export async function getDateRange(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "portfolio") {
    const company_id = params[0];
    const sql = `SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      CONCAT(
                          DATE_FORMAT(usfl.created_at, '%b %e, %Y'),
                          ' - ',
                          DATE_FORMAT(NOW(), '%b %e, %Y')
                      ) AS date_range
                      FROM form_data_company${company_id} AS fdc
                      LEFT JOIN user_stakeholder_form_logs usfl
                        ON usfl.user_stakeholder_form_id = fdc.form_id
                        AND usfl.wave = fdc.wave_id
                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
                      WHERE usf.company = ?  
                        AND fdc.wave_id = 1
                        /*PACK_FILTERS*/
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "portfolio_segment") {
    const company_id = params[1];
    const sql = `SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      CONCAT(
                          DATE_FORMAT(usfl.created_at, '%b %e, %Y'),
                          ' - ',
                          DATE_FORMAT(NOW(), '%b %e, %Y')
                      ) AS date_range
                      FROM form_data_company${company_id} AS fdc
                      LEFT JOIN user_stakeholder_form_logs usfl
                        ON usfl.user_stakeholder_form_id = fdc.form_id
                      AND usfl.wave = fdc.wave_id
                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
                      WHERE usf.company = ?                        
                        AND fdc.wave_id = 1
                        /*PACK_FILTERS*/
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id]);
    //console.log(rows);
    return rows;
  }
  //  else if (suffix === "portfolio_wave") {
  //   const wave_id = params[0];
  //   const company_id = params[1];
  //   const sql = `WITH base AS (
  //                         SELECT
  //                           fdc.form_id,
  //                           fdc.wave_id,
  //                           usf.current_wave,
  //                           MIN(usfl.created_at) AS created_at
  //                         FROM form_data_company${company_id} AS fdc
  //                         LEFT JOIN user_stakeholder_form_logs usfl
  //                           ON usfl.user_stakeholder_form_id = fdc.form_id
  //                         AND usfl.wave = fdc.wave_id
  //                         INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
  //                         WHERE usf.company = ?
  //                         /*PACK_FILTERS*/
  //                         GROUP BY fdc.form_id, fdc.wave_id
  //                         ORDER BY fdc.wave_id ASC
  //                       ),
  //                       waves AS (
  //                         SELECT
  //                           form_id,
  //                           wave_id,
  //                           created_at,
  //                           LEAD(created_at) OVER (PARTITION BY form_id ORDER BY wave_id) AS next_wave_date,
  //                           MAX(wave_id) OVER (PARTITION BY form_id) AS max_wave
  //                         FROM base
  //                       )
  //                       SELECT
  //                         w.form_id,
  //                         w.wave_id,
  //                         DATE_FORMAT(w.created_at, '%b %e, %Y') AS start_date,
  //                         CASE
  //                           WHEN w.wave_id = w.max_wave
  //                             THEN DATE_FORMAT(NOW(), '%b %e, %Y')
  //                           ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
  //                         END AS end_date,
  //                         CONCAT(
  //                           DATE_FORMAT(w.created_at, '%b %e, %Y'),
  //                           ' - ',
  //                           CASE
  //                             WHEN w.wave_id = w.max_wave
  //                               THEN DATE_FORMAT(NOW(), '%b %e, %Y')
  //                             ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
  //                           END
  //                         ) AS date_range
  //                       FROM waves w
  //                       WHERE w.wave_id = ?`;
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [company_id, wave_id]);
  //   //console.log(rows);
  //   return rows;
  // } else if (suffix === "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const company_id = params[2];
  //   const sql = `WITH base AS (
  //                         SELECT
  //                           fdc.form_id,
  //                           fdc.wave_id,
  //                           usf.current_wave,
  //                           MIN(usfl.created_at) AS created_at
  //                         FROM form_data_company${company_id} AS fdc
  //                         LEFT JOIN user_stakeholder_form_logs usfl
  //                           ON usfl.user_stakeholder_form_id = fdc.form_id
  //                         AND usfl.wave = fdc.wave_id
  //                         INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
  //                         WHERE usf.company = ?
  //                         /*PACK_FILTERS*/
  //                         GROUP BY fdc.form_id, fdc.wave_id
  //                         ORDER BY fdc.wave_id ASC
  //                       ),
  //                       waves AS (
  //                         SELECT
  //                           form_id,
  //                           wave_id,
  //                           created_at,
  //                           LEAD(created_at) OVER (PARTITION BY form_id ORDER BY wave_id) AS next_wave_date,
  //                           MAX(wave_id) OVER (PARTITION BY form_id) AS max_wave
  //                         FROM base
  //                       )
  //                       SELECT
  //                         w.form_id,
  //                         w.wave_id,
  //                         DATE_FORMAT(w.created_at, '%b %e, %Y') AS start_date,
  //                         CASE
  //                           WHEN w.wave_id = w.max_wave
  //                             THEN DATE_FORMAT(NOW(), '%b %e, %Y')
  //                           ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
  //                         END AS end_date,
  //                         CONCAT(
  //                           DATE_FORMAT(w.created_at, '%b %e, %Y'),
  //                           ' - ',
  //                           CASE
  //                             WHEN w.wave_id = w.max_wave
  //                               THEN DATE_FORMAT(NOW(), '%b %e, %Y')
  //                             ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
  //                           END
  //                         ) AS date_range
  //                       FROM waves w
  //                       WHERE w.wave_id = ?`;
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [company_id, wave_id]);
  //   //console.log(rows);
  //   return rows;
  // }
}

/** 7) getResponseRateNResponsesCount */
export async function getResponseRateNResponsesCount(filters) {
  return Promise.all([getSentCount(filters), getResponsesCount(filters)]).then(
    ([sentCount, responsesCount]) => {
      //console.log("sentCount:", sentCount, "responsesCount:", responsesCount);
      // Calculate percentage

      const percentage =
        sentCount.current_sent_count > 0
          ? Math.round(
              (responsesCount.current_response_count /
                sentCount.current_sent_count) *
                100
            )
          : "0";

      const percentageOld =
        sentCount.previous_sent_count > 0
          ? Math.round(
              (responsesCount.previous_response_count /
                sentCount.previous_sent_count) *
                100
            )
          : "0";
      // console.log("percentage:", percentageOld.toFixed(2) + "%");
      const percentageDelta = Math.round(percentage - percentageOld) + "%";

      // Example usage / return
      // console.log(percentageDisplay, percentageOldDisplay, percentageDeltaDisplay);
      return {
        sentCount: sentCount.current_sent_count,
        responsesCount: responsesCount.current_response_count,
        percentage: percentage + "%",
        percentageOld: percentageOld + "%",
        percentageDelta,
      };
      //return { percentage, sentCount, responsesCount };
    }
  );

  async function getSentCount(filters) {
    const { suffix, params } = pickVariant(filters);
    //
    let baseSql,
      queryParams = [];
    if (suffix == "portfolio") {
      const company_id = params[0];
      baseSql = `
                SELECT COUNT(*) AS count_occurrences
                FROM (
                    SELECT
                        TRIM(
                            SUBSTRING_INDEX(
                                SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n),
                                ',',
                                -1
                            )
                        ) AS sent_id
                    FROM user_stakeholder_form_logs AS usfl
                    INNER JOIN user_stakeholder_form AS usf
                        ON usf.id = usfl.user_stakeholder_form_id
                        AND usf.company = ?
                    JOIN numbers
                        ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                    WHERE 1=1
                    /*EXTRA_FILTERS_DATE*/
                    /*PACK_FILTERS*/
                ) AS split_ids
                INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
                WHERE 1=1
                /*EXTRA_FILTERS*/
                `;
      //console.log(baseSql);
      queryParams = [company_id];
    } else if (suffix == "portfolio_wave") {
      const wave_id = params[0];
      const company_id = params[1];
      baseSql = `
                SELECT COUNT(*) AS count_occurrences
                FROM (
                    SELECT
                        TRIM(
                            SUBSTRING_INDEX(
                                SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n),
                                ',',
                                -1
                            )
                        ) AS sent_id
                    FROM user_stakeholder_form_logs AS usfl
                    INNER JOIN user_stakeholder_form AS usf
                        ON usf.id = usfl.user_stakeholder_form_id
                        AND usf.company = ?
                    JOIN numbers
                        ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                    WHERE usfl.wave=?
                    /*EXTRA_FILTERS_DATE*/
                    /*PACK_FILTERS*/
                ) AS split_ids
                INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
                WHERE 1=1
                /*EXTRA_FILTERS*/
                `;
      //console.log(baseSql);
      queryParams = [company_id, wave_id];
    }
    // else if (suffix == "portfolio_segment") {
    //   const segment_id = params[0];
    //   const company_id = params[1];
    //   baseSql = `
    //             SELECT COUNT(*) AS count_occurrences
    //             FROM (
    //                 SELECT
    //                     TRIM(
    //                         SUBSTRING_INDEX(
    //                             SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n),
    //                             ',',
    //                             -1
    //                         )
    //                     ) AS sent_id
    //                 FROM user_stakeholder_form_logs AS usfl
    //                 INNER JOIN user_stakeholder_form AS usf
    //                     ON usf.id = usfl.user_stakeholder_form_id
    //                     AND usf.company = ?
    //                 JOIN numbers
    //                     ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
    //                 WHERE 1=1
    //                 /*EXTRA_FILTERS_DATE*/
    //                 /*PACK_FILTERS*/
    //             ) AS split_ids
    //             INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
    //             INNER JOIN client_segment_users AS csu
    //                 ON csu.segment_id = ?         -- e.g. 23
    //               AND FIND_IN_SET(split_ids.sent_id, csu.client_users) > 0
    //           WHERE 1=1
    //             /*EXTRA_FILTERS*/
    //             `;
    //   //console.log(baseSql1);
    //   queryParams = [company_id, segment_id];
    // } else if (suffix == "portfolio_wave_segment") {
    //   const wave_id = params[0];
    //   const segment_id = params[1];
    //   const company_id = params[2];
    //   baseSql = `
    //             SELECT COUNT(*) AS count_occurrences
    //             FROM (
    //                 SELECT
    //                     TRIM(
    //                         SUBSTRING_INDEX(
    //                             SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n),
    //                             ',',
    //                             -1
    //                         )
    //                     ) AS sent_id
    //                 FROM user_stakeholder_form_logs AS usfl
    //                 INNER JOIN user_stakeholder_form AS usf
    //                     ON usf.id = usfl.user_stakeholder_form_id
    //                     AND usf.company = ?
    //                 JOIN numbers
    //                     ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
    //                 WHERE usfl.wave = ?
    //                 /*EXTRA_FILTERS_DATE*/
    //                 /*PACK_FILTERS*/
    //             ) AS split_ids
    //             INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
    //             INNER JOIN client_segment_users AS csu
    //                 ON csu.segment_id = ?         -- e.g. 23
    //               AND FIND_IN_SET(split_ids.sent_id, csu.client_users) > 0
    //           WHERE 1=1
    //             /*EXTRA_FILTERS*/
    //             `;
    //   //console.log(baseSql1);
    //   queryParams = [company_id, wave_id, segment_id];
    // }
    // Apply filters
    let sql1 = baseSql.replace("/*EXTRA_FILTERS_DATE*/", "");
    const { sql } = applyExtraFilters(sql1, filters);

    let sql2 = baseSql.replace(
      "/*EXTRA_FILTERS_DATE*/",
      ` AND usfl.created_at < DATE_SUB(CURDATE(), INTERVAL 14 DAY)`
    );
    const { sql: oldSql } = applyExtraFilters(sql2, filters);

    // Current Sent score (latest)
    const [currentRows] = await pool.query(sql, queryParams);
    //console.log("Sent SQL:", sql);
    const currentSentCount = currentRows?.[0]?.count_occurrences || 0;
    //console.log("Current Sent Score:", currentSentCount);
    // Sent score before 14 days

    const [oldRows] = await pool.query(oldSql, queryParams);
    //console.log("Old Sent SQL:", oldSql);
    const oldSentCount = oldRows?.[0]?.count_occurrences || 0;
    //console.log("Old Sent Score:", oldSentCount);
    return {
      current_sent_count: currentSentCount,
      previous_sent_count: oldSentCount,
    };
  }
  async function getResponsesCount(filters) {
    const { suffix, params } = pickVariant(filters);
    //
    let baseSql,
      queryParams = [];
    if (suffix == "portfolio") {
      const company_id = params[0];
      baseSql = `WITH base AS (
           SELECT
              fdc.form_id,
              fdc.wave_id,
              COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
            FROM form_data_company${company_id} AS fdc
            INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
            INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
            WHERE usf.company = ?
             /*EXTRA_FILTERS*/
             /*PACK_FILTERS*/
            GROUP BY fdc.form_id, fdc.wave_id ORDER BY fdc.wave_id ASC
            )
            SELECT
            SUM(b.sum_wave_level_respondents) AS total_users_sum
        FROM base b
        `;
      //console.log(baseSql);
      queryParams = [company_id];
    } else if (suffix == "portfolio_wave") {
      const wave_id = params[0];
      const company_id = params[1];
      baseSql = `
              WITH base AS (
           SELECT
              fdc.form_id,
              fdc.wave_id,
              COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
            FROM form_data_company${company_id} AS fdc
            INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
            INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
            WHERE usf.company = ? AND fdc.wave_id = ?
             /*EXTRA_FILTERS*/
             /*PACK_FILTERS*/
            GROUP BY fdc.form_id, fdc.wave_id ORDER BY fdc.wave_id ASC
            )
            SELECT
            b.form_id,
            SUM(b.sum_wave_level_respondents) AS total_users_sum
        FROM base b          
            `;
      //console.log(baseSql);
      queryParams = [company_id, wave_id];
    }
    // else if (suffix == "portfolio_segment") {
    //   const segment_id = params[0];
    //   const company_id = params[1];
    //   baseSql = `
    //           WITH base AS (
    //        SELECT
    //           fdc.form_id,
    //           fdc.wave_id,
    //           COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
    //         FROM form_data_company${company_id} AS fdc
    //         INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
    //         INNER JOIN client_segment_users AS csu
    //              ON csu.company =?
    //             AND csu.segment_id = ?
    //             AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
    // 		WHERE 1=1
    //          /*EXTRA_FILTERS*/
    //          /*PACK_FILTERS*/
    //         GROUP BY fdc.form_id, fdc.wave_id ORDER BY fdc.wave_id ASC
    //         )
    //         SELECT
    //         b.form_id,
    //         SUM(b.sum_wave_level_respondents) AS total_users_sum
    //     FROM base b;
    //         `;
    //   //console.log(baseSql);
    //   queryParams = [company_id, segment_id];
    // } else if (suffix == "portfolio_wave_segment") {
    //   const wave_id = params[0];
    //   const segment_id = params[1];
    //   const company_id = params[2];
    //   baseSql = `
    //           WITH base AS (
    //        SELECT
    //           fdc.form_id,
    //           fdc.wave_id,
    //           COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
    //         FROM form_data_company${company_id} AS fdc
    //         INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
    //         INNER JOIN client_segment_users AS csu
    //              ON csu.segment_id = ?
    //             AND csu.company = ?
    //             AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
    // 		INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
    //         WHERE usf.company = ? AND fdc.wave_id = ?
    //          /*EXTRA_FILTERS*/
    //          /*PACK_FILTERS*/
    //         GROUP BY fdc.form_id, fdc.wave_id ORDER BY fdc.wave_id ASC
    //         )
    //         SELECT
    //         b.form_id,
    //         SUM(b.sum_wave_level_respondents) AS total_users_sum
    //     FROM base b
    //         `;
    //   //console.log(baseSql);
    //   queryParams = [segment_id, company_id, company_id, wave_id];
    // }
    // Apply filters
    const { sql } = applyExtraFilters(baseSql, filters);
    // Current Response Count (latest)
    const [currentRows] = await pool.query(sql, queryParams);
    //console.log("sql:", sql);
    //console.log("currentRows:", currentRows);
    const currentResponseCount = currentRows?.[0]?.total_users_sum || 0;
    //console.log("Current Response Count:", currentResponseCount);
    // Response Count before 14 days

    filters.dateFilter = "DATE_SUB(CURDATE(), INTERVAL 14 DAY)";
    const { sql: oldSql } = applyExtraFilters(baseSql, filters);
    // console.log("Old Response Count SQL:", oldSql);
    const [oldRows] = await pool.query(oldSql, queryParams);
    const oldResponseCount = oldRows?.[0]?.total_users_sum || 0;
    // console.log("Old Response Count:", oldResponseCount);
    return {
      current_response_count: currentResponseCount,
      previous_response_count: oldResponseCount,
    };
  }
}

/** 8) Charity Amount */
export async function getCharityAmount(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];

  if (suffix == "portfolio") {
    const company_id = params[0];
    baseSql = `
    WITH base AS (
           SELECT
              fdc.form_id,
              fdc.wave_id,
              COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents,
              (COUNT(DISTINCT(fdc.respondent_id)) * ucl.donation_amount) AS sum_wave_level_charity,
              ucl.donation_amount
            FROM form_data_company${company_id} AS fdc
            INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
            INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
            INNER JOIN user_charity_links AS ucl ON ucl.company = usf.company
            WHERE usf.company = ?
            /*EXTRA_FILTERS*/
            /*PACK_FILTERS*/
            GROUP BY fdc.form_id, fdc.wave_id  
            ORDER BY fdc.form_id ASC
            )
            SELECT
            SUM(b.sum_wave_level_charity) AS total_users_charity
        FROM base b
  `;
    //console.log(baseSql);
    queryParams = [company_id];
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    baseSql = `
    WITH base AS (
           SELECT
              fdc.form_id,
              fdc.wave_id,
              COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents,
              (COUNT(DISTINCT(fdc.respondent_id)) * ucl.donation_amount) AS sum_wave_level_charity,
              ucl.donation_amount
            FROM form_data_company${company_id} AS fdc
            INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
            INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
            INNER JOIN user_charity_links AS ucl ON ucl.company = usf.company
            WHERE usf.company = ? AND fdc.wave_id = ?
            /*EXTRA_FILTERS*/
            /*PACK_FILTERS*/
            GROUP BY fdc.form_id, fdc.wave_id  
            ORDER BY fdc.form_id ASC
            )
            SELECT
            SUM(b.sum_wave_level_charity) AS total_users_charity
        FROM base b
  `;
    //console.log(baseSql);
    queryParams = [company_id, wave_id];
  }
  //  else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   baseSql = `
  //   WITH base AS (
  //          SELECT
  //             fdc.form_id,
  //             fdc.wave_id,
  //             COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents,
  //             (COUNT(DISTINCT(fdc.respondent_id)) * ucl.donation_amount) AS sum_wave_level_charity,
  //   			    ucl.donation_amount
  //           FROM form_data_company${company_id} AS fdc
  //           INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //           INNER JOIN client_segment_users AS csu
  //                ON csu.company =?
  //               AND csu.segment_id = ?
  //               AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //           INNER JOIN user_charity_links AS ucl ON ucl.company = csu.company
  //           INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
  //   		WHERE 1=1
  //            /*EXTRA_FILTERS*/
  //           /*PACK_FILTERS*/
  //           GROUP BY fdc.form_id, fdc.wave_id ORDER BY fdc.wave_id ASC
  //           )
  //           SELECT
  //           b.form_id,
  //           SUM(b.sum_wave_level_charity) AS total_users_charity
  //       FROM base b;
  // `;
  //   //console.log(baseSql);
  //   queryParams = [company_id, segment_id];
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   baseSql = `
  //   WITH base AS (
  //          SELECT
  //             fdc.form_id,
  //             fdc.wave_id,
  //             COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents,
  //             (COUNT(DISTINCT(fdc.respondent_id)) * ucl.donation_amount) AS sum_wave_level_charity,
  //   			    ucl.donation_amount
  //           FROM form_data_company${company_id} AS fdc
  //           INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //           INNER JOIN client_segment_users AS csu
  //                ON csu.company =?
  //               AND csu.segment_id = ?
  //               AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //           INNER JOIN user_charity_links AS ucl ON ucl.company = csu.company
  //           INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
  //   		WHERE fdc.wave_id = ?
  //            /*EXTRA_FILTERS*/
  //            /*PACK_FILTERS*/
  //           GROUP BY fdc.form_id, fdc.wave_id ORDER BY fdc.wave_id ASC
  //           )
  //           SELECT
  //           b.form_id,
  //           SUM(b.sum_wave_level_charity) AS total_users_charity
  //       FROM base b;
  // `;
  //   //console.log(baseSql);
  //   queryParams = [company_id, segment_id, wave_id];
  // }
  // Apply filters
  const { sql } = applyExtraFilters(baseSql, filters);
  // Current RAG score (latest)
  const [currentRows] = await pool.query(sql, queryParams);
  // console.log("RAG SQL:", sql);
  const charity = currentRows?.[0]?.total_users_charity || 0;
  //console.log("Current RAG Score:", currentScore);

  // Final response
  return {
    charityAmount: charity,
  };
}

/** 9) getBottom3Respondents */
export async function getBottom3Respondents(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    let company_id = params[0];

    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      fdc.wave_id,
                      fdc.respondent_id,
                      cu.name,
                      cu.role,
                      cu.organization,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.respondent_id
                    ORDER BY  fdc.respondent_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      wave_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY respondent_id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      stakeholder_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY respondent_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql);
    //console.log(rows);
    return rows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    //console.log("Wave: ", wave_id);
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      fdc.wave_id,
                      fdc.respondent_id,
                      cu.name,
                      cu.role,
                      cu.organization,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.respondent_id
                    ORDER BY  fdc.respondent_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      wave_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  )
                  SELECT
                      stakeholder_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_l AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    // console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [wave_id]);
    console.log(rows);
    return rows;
  }
  // else if (suffix == "portfolio_segment") {
  //   let segment_id = params[0];
  //   let company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     usf.stakeholder_id,
  //                     fdc.wave_id,
  //                     fdc.respondent_id,
  //                     cu.name,
  //                     cu.role,
  //                     cu.organization,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.respondent_id
  //                   ORDER BY  fdc.respondent_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     stakeholder_id,
  //                     wave_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY respondent_id) AS max_wave_for_q
  //                 FROM scored s
  //               )
  //                 SELECT
  //                     stakeholder_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY respondent_id
  //                   ORDER BY rag_score_avg_l ASC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id]);
  //   //console.log(rows);
  //   return rows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                  SELECT
  //                     usf.stakeholder_id,
  //                     fdc.wave_id,
  //                     fdc.respondent_id,
  //                     cu.name,
  //                     cu.role,
  //                     cu.organization,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.respondent_id
  //                   ORDER BY  fdc.respondent_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     stakeholder_id,
  //                     wave_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 )
  //                 SELECT
  //                     stakeholder_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_l AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     rag_score_avg_l,                           -- ragscore from latest wave
  //                     rag_score_avg_p,                            -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                   ORDER BY rag_score_avg_l ASC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, wave_id]);
  //   //console.log(rows);
  //   return rows;
  // }
}

/** 10) getTop3Respondents */
export async function getTop3Respondents(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    let company_id = params[0];

    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      fdc.wave_id,
                      fdc.respondent_id,
                      cu.name,
                      cu.role,
                      cu.organization,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.respondent_id
                    ORDER BY  fdc.respondent_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      wave_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY respondent_id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      stakeholder_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY respondent_id
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql);
    //console.log(rows);
    return rows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    console.log("Wave: ", wave_id);
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      fdc.wave_id,
                      fdc.respondent_id,
                      cu.name,
                      cu.role,
                      cu.organization,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.respondent_id
                    ORDER BY  fdc.respondent_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      wave_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  )
                  SELECT
                      stakeholder_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_l AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                `;
    // console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [wave_id]);
    console.log(rows);
    return rows;
  }
  // else if (suffix == "portfolio_segment") {
  //   let segment_id = params[0];
  //   let company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     usf.stakeholder_id,
  //                     fdc.wave_id,
  //                     fdc.respondent_id,
  //                     cu.name,
  //                     cu.role,
  //                     cu.organization,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.respondent_id
  //                   ORDER BY  fdc.respondent_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     stakeholder_id,
  //                     wave_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY respondent_id) AS max_wave_for_q
  //                 FROM scored s
  //               )
  //                 SELECT
  //                     stakeholder_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY respondent_id
  //                   ORDER BY rag_score_avg_l DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id]);
  //   //console.log(rows);
  //   return rows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                  SELECT
  //                     usf.stakeholder_id,
  //                     fdc.wave_id,
  //                     fdc.respondent_id,
  //                     cu.name,
  //                     cu.role,
  //                     cu.organization,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.respondent_id
  //                   ORDER BY  fdc.respondent_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     stakeholder_id,
  //                     wave_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 )
  //                 SELECT
  //                     stakeholder_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_l AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     rag_score_avg_l, -- ragscore from latest wave
  //                     rag_score_avg_p, -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                   ORDER BY rag_score_avg_l DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, wave_id]);
  //   //console.log(rows);
  //   return rows;
  // }
}

/** 11) getTrend3Respondents */
export async function getTrend3Respondents(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    let company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      fdc.wave_id,
                      fdc.respondent_id,
                      cu.name,
                      cu.role,
                      cu.organization,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.respondent_id
                    ORDER BY  fdc.respondent_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      wave_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY respondent_id) AS max_wave_for_q
                  FROM scored s
                ),
                final AS (
                  SELECT
                      stakeholder_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY respondent_id
                  )
                    SELECT * FROM final
                     ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql);
    //console.log(rows);
    return rows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    //console.log("Wave: ", wave_id);
    const company_id = params[1];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.stakeholder_id,
                      fdc.wave_id,
                      fdc.respondent_id,
                      cu.name,
                      cu.role,
                      cu.organization,                      
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.respondent_id
                    ORDER BY  fdc.respondent_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      wave_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                final AS(
                  SELECT
                      stakeholder_id,
                      respondent_id,
                      name,
                      role,
                      organization,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    GROUP BY respondent_id
                    )
                    SELECT * FROM final
                     ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [wave_id, wave_id, wave_id, wave_id]);
    //console.log(rows);
    return rows;
  }
  // else if (suffix == "portfolio_segment") {
  //   let segment_id = params[0];
  //   let company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     usf.stakeholder_id,
  //                     fdc.wave_id,
  //                     fdc.respondent_id,
  //                     cu.name,
  //                     cu.role,
  //                     cu.organization,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.respondent_id
  //                   ORDER BY  fdc.respondent_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     stakeholder_id,
  //                     wave_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY respondent_id) AS max_wave_for_q
  //                 FROM scored s
  //               ),
  //               final AS (
  //                 SELECT
  //                     stakeholder_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY respondent_id
  //                   )
  //                   SELECT * FROM final
  //                    ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id]);
  //   //console.log(rows);
  //   return rows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                  SELECT
  //                     usf.stakeholder_id,
  //                     fdc.wave_id,
  //                     fdc.respondent_id,
  //                     cu.name,
  //                     cu.role,
  //                     cu.organization,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.respondent_id
  //                   ORDER BY  fdc.respondent_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     stakeholder_id,
  //                     wave_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 ),
  //               final AS(
  //                 SELECT
  //                     stakeholder_id,
  //                     respondent_id,
  //                     name,
  //                     role,
  //                     organization,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     rag_score_avg_l, -- ragscore from latest wave
  //                     rag_score_avg_p, -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                   )
  //                   SELECT * FROM final
  //                    ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, wave_id]);
  //   //console.log(rows);
  //   return rows;
  // }
}

/** 12) getTop3Questions */
export async function getTop3Questions(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
    				          cu.stakeholder_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      ROUND(avg_score, 2) AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                    ORDER BY wave_id DESC
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY question_id
                    ORDER BY max_wave_for_q DESC, rag_score_avg_l DESC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql);
    //console.log(rows);
    return rows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
    				          cu.stakeholder_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      ROUND(avg_score, 2) AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                    ORDER BY wave_id DESC
                  )
                  SELECT
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [wave_id, wave_id, wave_id, wave_id]);
    //console.log(rows);
    return rows;
  }
  // else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     fdc.form_id,
  //                     fdc.wave_id,
  //                     fdc.question_id,
  //   				          cu.stakeholder_id,
  //                     q.three_word_outcome_en,
  //                     AVG(fdc.option_numeric) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   LEFT JOIN question q
  //                     ON q.id = fdc.question_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.question_id
  //                   ORDER BY fdc.question_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     form_id,
  //                     wave_id,
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                     ROUND(avg_score, 2) AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                   ORDER BY wave_id DESC
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
  //                 FROM scored s
  //               )
  //                 SELECT
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                    MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY question_id
  //                   ORDER BY max_wave_for_q DESC, rag_score_avg_l DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id]);
  //   //console.log(rows);
  //   return rows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     fdc.form_id,
  //                     fdc.wave_id,
  //                     fdc.question_id,
  //   				          cu.stakeholder_id,
  //                     q.three_word_outcome_en,
  //                     AVG(fdc.option_numeric) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   LEFT JOIN question q
  //                     ON q.id = fdc.question_id
  //                   INNER JOIN client_segment_users AS csu
  //                       ON csu.segment_id = ? AND csu.company=?
  //                       AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.question_id
  //                   ORDER BY fdc.question_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     form_id,
  //                     wave_id,
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                     ROUND(avg_score, 2) AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                   ORDER BY wave_id DESC
  //                 )
  //                 SELECT
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
  //                     rag_score_avg_l, -- ragscore from latest wave
  //                     rag_score_avg_p, -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                   ORDER BY rag_score_avg_l DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, wave_id]);
  //   //console.log(rows);
  //   return rows;
  // }
}

/** 13) getBottom3Questions */
export async function getBottom3Questions(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
    				          cu.stakeholder_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE 1=1            -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      ROUND(avg_score, 2) AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                    ORDER BY wave_id DESC
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY question_id
                    ORDER BY max_wave_for_q DESC, rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql);
    //console.log(rows);
    return rows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
    				          cu.stakeholder_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      ROUND(avg_score, 2) AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                    ORDER BY wave_id DESC
                  )
                  SELECT
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [wave_id]);
    //console.log(rows);
    return rows;
  }
  // else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     fdc.form_id,
  //                     fdc.wave_id,
  //                     fdc.question_id,
  //   				          cu.stakeholder_id,
  //                     q.three_word_outcome_en,
  //                     AVG(fdc.option_numeric) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   LEFT JOIN question q
  //                     ON q.id = fdc.question_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.question_id
  //                   ORDER BY fdc.question_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     form_id,
  //                     wave_id,
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                     ROUND(avg_score, 2) AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                   ORDER BY wave_id DESC
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
  //                 FROM scored s
  //               )
  //                 SELECT
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                    MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY question_id
  //                   ORDER BY rag_score_avg_l ASC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id]);
  //   //console.log(rows);
  //   return rows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     fdc.form_id,
  //                     fdc.wave_id,
  //                     fdc.question_id,
  //   				          cu.stakeholder_id,
  //                     q.three_word_outcome_en,
  //                     AVG(fdc.option_numeric) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   LEFT JOIN question q
  //                     ON q.id = fdc.question_id
  //                   INNER JOIN client_segment_users AS csu
  //                       ON csu.segment_id = ? AND csu.company=?
  //                       AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.question_id
  //                   ORDER BY fdc.question_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     form_id,
  //                     wave_id,
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                     ROUND(avg_score, 2) AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                   ORDER BY wave_id DESC
  //                 )
  //                 SELECT
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
  //                     rag_score_avg_l, -- ragscore from latest wave
  //                     rag_score_avg_p, -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                   ORDER BY rag_score_avg_l ASC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, wave_id]);
  //   //console.log(rows);
  //   return rows;
  // }
}

/** 14) getTrendMoversQuestions */
export async function getTrendMoversQuestions(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
    				          cu.stakeholder_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      ROUND(avg_score, 2) AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                    ORDER BY wave_id DESC
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM scored s
                ),
                final AS (
                  SELECT
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY question_id
                  )
                    SELECT * FROM final
                     ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql);
    //console.log(rows);
    return rows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
    				          cu.stakeholder_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      ROUND(avg_score, 2) AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                    ORDER BY wave_id DESC
                  ),
                final AS (
                  SELECT
                      question_id,
                      stakeholder_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                  )
                    SELECT * FROM final
                     ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [wave_id]);
    //console.log(rows);
    return rows;
  }
  // else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     fdc.form_id,
  //                     fdc.wave_id,
  //                     fdc.question_id,
  //   				          cu.stakeholder_id,
  //                     q.three_word_outcome_en,
  //                     ROUND(AVG(fdc.option_numeric), 2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   LEFT JOIN question q
  //                     ON q.id = fdc.question_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ?            -- e.g. 23
  //                   AND csu.company = ?               -- e.g. 2
  //                   AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.question_id
  //                   ORDER BY fdc.question_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     form_id,
  //                     wave_id,
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                     ROUND(avg_score, 2) AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                   ORDER BY wave_id DESC
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
  //                 FROM scored s
  //               ),
  //               final AS (
  //                 SELECT
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                    MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY question_id
  //                 )
  //                   SELECT * FROM final
  //                    ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id]);
  //   //console.log(rows);
  //   return rows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     fdc.form_id,
  //                     fdc.wave_id,
  //                     fdc.question_id,
  //   				          cu.stakeholder_id,
  //                     q.three_word_outcome_en,
  //                     ROUND(AVG(fdc.option_numeric), 2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   LEFT JOIN question q
  //                     ON q.id = fdc.question_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ?            -- e.g. 23
  //                   AND csu.company = ?               -- e.g. 2
  //                   AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   WHERE fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, fdc.question_id
  //                   ORDER BY fdc.question_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     form_id,
  //                     wave_id,
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                     ROUND(avg_score, 2) AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                   ORDER BY wave_id DESC
  //                 ),
  //               final AS (
  //                 SELECT
  //                     question_id,
  //                     stakeholder_id,
  //                     three_word_outcome_en,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
  //                     rag_score_avg_l, -- ragscore from latest wave
  //                     rag_score_avg_p, -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                 )
  //                   SELECT * FROM final
  //                    ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, wave_id]);
  //   //console.log(rows);
  //   return rows;
  // }
}

/** 15) getTop3Packs */
export async function getTop3Packs(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.subcategory_id,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.name as subcategory_name,
                      usf.category_id,
                      cat.name as category_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      wave_id,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY subcategory_id) AS max_wave_for_q
                  FROM scored s
                ),
                final AS (              
                  SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY subcategory_id
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                )
                    SELECT
                      f.subcategory_id,
                      f.subcategory_formatted_code,
                      f.subcategory_name,
                      f.category_id,
                      f.category_name,
                      f.rag_score_avg_l,
                      f.rag_score_avg_p,
                      f.delta_vs_prev_avg,
                      GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
                    FROM final AS f
                    LEFT JOIN user_stakeholder_form usf
                      ON usf.subcategory_id = f.subcategory_id
                      AND usf.company = ${company_id}
                    GROUP BY f.subcategory_id
                    ORDER BY f.rag_score_avg_l DESC
                `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id]);
    //console.log(rows);
    const enrichedRows = await Promise.all(
      rows.map((row) => enrichWithStakeholders(row, company_id))
    );
    // console.log(enrichedRows);
    return enrichedRows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.subcategory_id,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.name as subcategory_name,
                      usf.category_id,
                      cat.name as category_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      wave_id,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                ),
                final AS (                  
                  SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                  )
                    SELECT
                      f.subcategory_id,
                      f.subcategory_formatted_code,
                      f.subcategory_name,
                      f.category_id,
                      f.category_name,
                      f.rag_score_avg_l,
                      f.rag_score_avg_p,
                      f.delta_vs_prev_avg,
                      GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
                    FROM final AS f
                    LEFT JOIN user_stakeholder_form usf
                      ON usf.subcategory_id = f.subcategory_id
                      AND usf.company = ${company_id}
                    GROUP BY f.subcategory_id
                    ORDER BY f.rag_score_avg_l DESC
                `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id, wave_id]);
    //console.log(rows);
    const enrichedRows = await Promise.all(
      rows.map((row) => enrichWithStakeholders(row, company_id))
    );
    // console.log(enrichedRows);
    return enrichedRows;
  }
  // else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     usf.subcategory_id,
  //                     CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
  //                     sub.name as subcategory_name,
  //                     usf.category_id,
  //                     cat.name as category_name,
  //                     fdc.wave_id,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     wave_id,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY subcategory_id) AS max_wave_for_q
  //                 FROM scored s
  //               ),
  //               final AS (
  //                 SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                    MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY subcategory_id
  //                   ORDER BY rag_score_avg_l DESC LIMIT 3
  //                   )
  //                   SELECT
  //                     f.subcategory_id,
  //                     f.subcategory_formatted_code,
  //                     f.subcategory_name,
  //                     f.category_id,
  //                     f.category_name,
  //                     f.rag_score_avg_l,
  //                     f.rag_score_avg_p,
  //                     f.delta_vs_prev_avg,
  //                     GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
  //                   FROM final AS f
  //                   LEFT JOIN user_stakeholder_form usf
  //                     ON usf.subcategory_id = f.subcategory_id
  //                     AND usf.company = ${company_id}
  //                   GROUP BY f.subcategory_id
  //                   ORDER BY f.rag_score_avg_l DESC
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, company_id]);
  //   //console.log(rows);
  //   const enrichedRows = await Promise.all(
  //     rows.map((row) => enrichWithStakeholders(row, company_id))
  //   );
  //   // console.log(enrichedRows);
  //   return enrichedRows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                  SELECT
  //                     usf.subcategory_id,
  //                     CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
  //                     sub.name as subcategory_name,
  //                     usf.category_id,
  //                     cat.name as category_name,
  //                     fdc.wave_id,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     wave_id,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //               ),
  //               final AS (
  //                 SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     rag_score_avg_l, -- ragscore from latest wave
  //                     rag_score_avg_p, -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                   ORDER BY rag_score_avg_l DESC LIMIT 3
  //                   )
  //                   SELECT
  //                     f.subcategory_id,
  //                     f.subcategory_formatted_code,
  //                     f.subcategory_name,
  //                     f.category_id,
  //                     f.category_name,
  //                     f.rag_score_avg_l,
  //                     f.rag_score_avg_p,
  //                     f.delta_vs_prev_avg,
  //                     GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
  //                   FROM final AS f
  //                   LEFT JOIN user_stakeholder_form usf
  //                     ON usf.subcategory_id = f.subcategory_id
  //                     AND usf.company = ${company_id}
  //                   GROUP BY f.subcategory_id
  //                   ORDER BY f.rag_score_avg_l DESC
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [
  //     segment_id,
  //     company_id,
  //     company_id,
  //     wave_id,
  //   ]);
  //   //console.log(rows);
  //   const enrichedRows = await Promise.all(
  //     rows.map((row) => enrichWithStakeholders(row, company_id))
  //   );
  //   // console.log(enrichedRows);
  //   return enrichedRows;
  // }
}

/** 16) getBottom3Packs */
export async function getBottom3Packs(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.subcategory_id,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.name as subcategory_name,
                      usf.category_id,
                      cat.name as category_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      wave_id,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY subcategory_id) AS max_wave_for_q
                  FROM scored s
                ),
                final AS (              
                  SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY subcategory_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                )
                    SELECT
                      f.subcategory_id,
                      f.subcategory_formatted_code,
                      f.subcategory_name,
                      f.category_id,
                      f.category_name,
                      f.rag_score_avg_l,
                      f.rag_score_avg_p,
                      f.delta_vs_prev_avg,
                      GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
                    FROM final AS f
                    LEFT JOIN user_stakeholder_form usf
                      ON usf.subcategory_id = f.subcategory_id
                      AND usf.company = ${company_id}
                    GROUP BY f.subcategory_id
                    ORDER BY f.rag_score_avg_l ASC
                `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id]);
    //console.log(rows);
    const enrichedRows = await Promise.all(
      rows.map((row) => enrichWithStakeholders(row, company_id))
    );
    // console.log(enrichedRows);
    return enrichedRows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.subcategory_id,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.name as subcategory_name,
                      usf.category_id,
                      cat.name as category_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      wave_id,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                ),
                final AS (                  
                  SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                  )
                    SELECT
                      f.subcategory_id,
                      f.subcategory_formatted_code,
                      f.subcategory_name,
                      f.category_id,
                      f.category_name,
                      f.rag_score_avg_l,
                      f.rag_score_avg_p,
                      f.delta_vs_prev_avg,
                      GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
                    FROM final AS f
                    LEFT JOIN user_stakeholder_form usf
                      ON usf.subcategory_id = f.subcategory_id
                      AND usf.company = ${company_id}
                    GROUP BY f.subcategory_id
                    ORDER BY f.rag_score_avg_l ASC
                `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id, wave_id]);
    //console.log(rows);
    const enrichedRows = await Promise.all(
      rows.map((row) => enrichWithStakeholders(row, company_id))
    );
    // console.log(enrichedRows);
    return enrichedRows;
  }
  // else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     usf.subcategory_id,
  //                     CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
  //                     sub.name as subcategory_name,
  //                     usf.category_id,
  //                     cat.name as category_name,
  //                     fdc.wave_id,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     wave_id,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY subcategory_id) AS max_wave_for_q
  //                 FROM scored s
  //               ),
  //               final AS (
  //                 SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                    MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY subcategory_id
  //                   ORDER BY rag_score_avg_l ASC LIMIT 3
  //                   )
  //                   SELECT
  //                     f.subcategory_id,
  //                     f.subcategory_formatted_code,
  //                     f.subcategory_name,
  //                     f.category_id,
  //                     f.category_name,
  //                     f.rag_score_avg_l,
  //                     f.rag_score_avg_p,
  //                     f.delta_vs_prev_avg,
  //                     GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
  //                   FROM final AS f
  //                   LEFT JOIN user_stakeholder_form usf
  //                     ON usf.subcategory_id = f.subcategory_id
  //                     AND usf.company = ${company_id}
  //                   GROUP BY f.subcategory_id
  //                   ORDER BY f.rag_score_avg_l ASC
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, company_id]);
  //   //console.log(rows);
  //   const enrichedRows = await Promise.all(
  //     rows.map((row) => enrichWithStakeholders(row, company_id))
  //   );
  //   // console.log(enrichedRows);
  //   return enrichedRows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                  SELECT
  //                     usf.subcategory_id,
  //                     CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
  //                     sub.name as subcategory_name,
  //                     usf.category_id,
  //                     cat.name as category_name,
  //                     fdc.wave_id,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     wave_id,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //               ),
  //               final AS (
  //                 SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     rag_score_avg_l, -- ragscore from latest wave
  //                     rag_score_avg_p, -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                   ORDER BY rag_score_avg_l ASC LIMIT 3
  //                   )
  //                   SELECT
  //                     f.subcategory_id,
  //                     f.subcategory_formatted_code,
  //                     f.subcategory_name,
  //                     f.category_id,
  //                     f.category_name,
  //                     f.rag_score_avg_l,
  //                     f.rag_score_avg_p,
  //                     f.delta_vs_prev_avg,
  //                     GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
  //                   FROM final AS f
  //                   LEFT JOIN user_stakeholder_form usf
  //                     ON usf.subcategory_id = f.subcategory_id
  //                     AND usf.company = ${company_id}
  //                   GROUP BY f.subcategory_id
  //                   ORDER BY f.rag_score_avg_l ASC
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [
  //     segment_id,
  //     company_id,
  //     company_id,
  //     wave_id,
  //   ]);
  //   //console.log(rows);
  //   const enrichedRows = await Promise.all(
  //     rows.map((row) => enrichWithStakeholders(row, company_id))
  //   );
  //   // console.log(enrichedRows);
  //   return enrichedRows;
  // }
}

/** 17) getTrendMovers */
export async function getTrendMoversPacks(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.subcategory_id,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.name as subcategory_name,
                      usf.category_id,
                      cat.name as category_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      wave_id,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY subcategory_id) AS max_wave_for_q
                  FROM scored s
                ),
                final AS (
                  SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY subcategory_id
                    )
                    SELECT
                      f.subcategory_id,
                      f.subcategory_formatted_code,
                      f.subcategory_name,
                      f.category_id,
                      f.category_name,
                      f.rag_score_avg_l,
                      f.rag_score_avg_p,
                      f.delta_vs_prev_avg,
                      GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
                    FROM final AS f
                    LEFT JOIN user_stakeholder_form usf
                      ON usf.subcategory_id = f.subcategory_id
                      AND usf.company = ${company_id}
                    GROUP BY f.subcategory_id
                    ORDER BY ABS(f.delta_vs_prev_avg) DESC LIMIT 3                  
                `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id]);
    //console.log(rows);
    const enrichedRows = await Promise.all(
      rows.map((row) => enrichWithStakeholders(row, company_id))
    );
    // console.log(enrichedRows);
    return enrichedRows;
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.subcategory_id,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.name as subcategory_name,
                      usf.category_id,
                      cat.name as category_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      wave_id,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                final AS (
                  SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    )
                    SELECT
                      f.subcategory_id,
                      f.subcategory_formatted_code,
                      f.subcategory_name,
                      f.category_id,
                      f.category_name,
                      f.rag_score_avg_l,
                      f.rag_score_avg_p,
                      f.delta_vs_prev_avg,
                      GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
                    FROM final AS f
                    LEFT JOIN user_stakeholder_form usf
                      ON usf.subcategory_id = f.subcategory_id
                      AND usf.company = ${company_id}
                    GROUP BY f.subcategory_id
                    ORDER BY ABS(f.delta_vs_prev_avg) DESC LIMIT 3         
                `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id, wave_id]);
    //console.log(rows);
    const enrichedRows = await Promise.all(
      rows.map((row) => enrichWithStakeholders(row, company_id))
    );
    // console.log(enrichedRows);
    return enrichedRows;
  }
  // else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     usf.subcategory_id,
  //                     CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
  //                     sub.name as subcategory_name,
  //                     usf.category_id,
  //                     cat.name as category_name,
  //                     fdc.wave_id,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     wave_id,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY subcategory_id) AS max_wave_for_q
  //                 FROM scored s
  //               ),
  //               final AS (
  //                 SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY subcategory_id
  //                   )
  //                   SELECT
  //                     f.subcategory_id,
  //                     f.subcategory_formatted_code,
  //                     f.subcategory_name,
  //                     f.category_id,
  //                     f.category_name,
  //                     f.rag_score_avg_l,
  //                     f.rag_score_avg_p,
  //                     f.delta_vs_prev_avg,
  //                     GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
  //                   FROM final AS f
  //                   LEFT JOIN user_stakeholder_form usf
  //                     ON usf.subcategory_id = f.subcategory_id
  //                     AND usf.company = ${company_id}
  //                   GROUP BY f.subcategory_id
  //                   ORDER BY ABS(f.delta_vs_prev_avg) DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, company_id]);
  //   //console.log(rows);
  //   const enrichedRows = await Promise.all(
  //     rows.map((row) => enrichWithStakeholders(row, company_id))
  //   );
  //   // console.log(enrichedRows);
  //   return enrichedRows;
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                  SELECT
  //                     usf.subcategory_id,
  //                     CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
  //                     sub.name as subcategory_name,
  //                     usf.category_id,
  //                     cat.name as category_name,
  //                     fdc.wave_id,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     wave_id,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 ),
  //               final AS (
  //                 SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     rag_score_avg_l, -- ragscore from latest wave
  //                     rag_score_avg_p, -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                   )
  //                   SELECT
  //                     f.subcategory_id,
  //                     f.subcategory_formatted_code,
  //                     f.subcategory_name,
  //                     f.category_id,
  //                     f.category_name,
  //                     f.rag_score_avg_l,
  //                     f.rag_score_avg_p,
  //                     f.delta_vs_prev_avg,
  //                     GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
  //                   FROM final AS f
  //                   LEFT JOIN user_stakeholder_form usf
  //                     ON usf.subcategory_id = f.subcategory_id
  //                     AND usf.company = ${company_id}
  //                   GROUP BY f.subcategory_id
  //                   ORDER BY ABS(f.delta_vs_prev_avg) DESC LIMIT 3
  //               `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [
  //     segment_id,
  //     company_id,
  //     company_id,
  //     wave_id,
  //   ]);
  //   //console.log(rows);
  //   const enrichedRows = await Promise.all(
  //     rows.map((row) => enrichWithStakeholders(row, company_id))
  //   );
  //   // console.log(enrichedRows);
  //   return enrichedRows;
  // }
}

/** 18) getAllPacks */
export async function getAllPacks(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "portfolio") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.subcategory_id,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.name as subcategory_name,
                      usf.category_id,
                      cat.name as category_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      wave_id,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY subcategory_id) AS max_wave_for_q
                  FROM scored s
                ),
                final AS (                  
                  SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                     MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY subcategory_id
                    ORDER BY rag_score_avg_l DESC -- LIMIT 3
                 )
                    SELECT
                      f.subcategory_id,
                      f.subcategory_formatted_code,
                      f.subcategory_name,
                      f.category_id,
                      f.category_name,
                      f.rag_score_avg_l,
                      f.rag_score_avg_p,
                      f.delta_vs_prev_avg,
                      GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
                    FROM final AS f
                    LEFT JOIN user_stakeholder_form usf
                      ON usf.subcategory_id = f.subcategory_id
                      AND usf.company = ${company_id}
                    GROUP BY f.subcategory_id
                    ORDER BY f.rag_score_avg_l DESC
                    `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id]);
    //console.log(rows);
    // Process data concurrently
    const [enrichedRows, uniqueCategories] = await Promise.all([
      Promise.all(rows.map((row) => enrichWithStakeholders(row, company_id))),
      Promise.resolve(extractDistinctCategories(rows)),
    ]);
    // console.log(enrichedRows);
    // console.log(uniqueCategories);
    // Use results
    return {
      categories: uniqueCategories,
      enrichedRows: enrichedRows,
    };
  } else if (suffix == "portfolio_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.subcategory_id,
                      CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
                      sub.name as subcategory_name,
                      usf.category_id,
                      cat.name as category_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    /*PACK_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      wave_id,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                ),
                final AS (            
                  SELECT
                      subcategory_id,
                      subcategory_formatted_code,
                      subcategory_name,
                      category_id,
                      category_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l DESC -- LIMIT 3
                )
                    SELECT
                      f.subcategory_id,
                      f.subcategory_formatted_code,
                      f.subcategory_name,
                      f.category_id,
                      f.category_name,
                      f.rag_score_avg_l,
                      f.rag_score_avg_p,
                      f.delta_vs_prev_avg,
                      GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
                    FROM final AS f
                    LEFT JOIN user_stakeholder_form usf
                      ON usf.subcategory_id = f.subcategory_id
                      AND usf.company = ${company_id}
                    GROUP BY f.subcategory_id
                    ORDER BY f.rag_score_avg_l DESC
                    `;
    //console.log(baseSql);
    const { sql } = await applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [company_id, wave_id]);
    //console.log(rows);
    // Process data concurrently
    const [enrichedRows, uniqueCategories] = await Promise.all([
      Promise.all(rows.map((row) => enrichWithStakeholders(row, company_id))),
      Promise.resolve(extractDistinctCategories(rows)),
    ]);
    // console.log(enrichedRows);
    // console.log(uniqueCategories);
    // Use results
    return {
      categories: uniqueCategories,
      enrichedRows: enrichedRows,
    };
  }
  // else if (suffix == "portfolio_segment") {
  //   const segment_id = params[0];
  //   const company_id = params[1];
  //   const baseSql = `WITH base AS (
  //                   SELECT
  //                     usf.subcategory_id,
  //                     CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
  //                     sub.name as subcategory_name,
  //                     usf.category_id,
  //                     cat.name as category_name,
  //                     fdc.wave_id,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     wave_id,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //                 ),
  //               scored_with_max AS (
  //                 SELECT
  //                   s.*,
  //                   MAX(wave_id) OVER (PARTITION BY subcategory_id) AS max_wave_for_q
  //                 FROM scored s
  //               ),
  //               final AS (
  //                 SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
  //                     MAX(CASE WHEN wave_id = max_wave_for_q
  //                             THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored_with_max
  //                   GROUP BY subcategory_id
  //                   ORDER BY rag_score_avg_l DESC -- LIMIT 3
  //               )
  //                   SELECT
  //                     f.subcategory_id,
  //                     f.subcategory_formatted_code,
  //                     f.subcategory_name,
  //                     f.category_id,
  //                     f.category_name,
  //                     f.rag_score_avg_l,
  //                     f.rag_score_avg_p,
  //                     f.delta_vs_prev_avg,
  //                     GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
  //                   FROM final AS f
  //                   LEFT JOIN user_stakeholder_form usf
  //                     ON usf.subcategory_id = f.subcategory_id
  //                     AND usf.company = ${company_id}
  //                   GROUP BY f.subcategory_id
  //                   ORDER BY f.rag_score_avg_l DESC
  //                   `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [segment_id, company_id, company_id]);
  //   //console.log(rows);
  //   // Process data concurrently
  //   const [enrichedRows, uniqueCategories] = await Promise.all([
  //     Promise.all(rows.map((row) => enrichWithStakeholders(row, company_id))),
  //     Promise.resolve(extractDistinctCategories(rows)),
  //   ]);
  //   // console.log(enrichedRows);
  //   // console.log(uniqueCategories);
  //   // Use results
  //   return {
  //     categories: uniqueCategories,
  //     enrichedRows: enrichedRows,
  //   };
  // } else if (suffix == "portfolio_wave_segment") {
  //   const wave_id = params[0];
  //   const segment_id = params[1];
  //   const company_id = params[2];
  //   const baseSql = `WITH base AS (
  //                  SELECT
  //                     usf.subcategory_id,
  //                     CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
  //                     sub.name as subcategory_name,
  //                     usf.category_id,
  //                     cat.name as category_name,
  //                     fdc.wave_id,
  //                     ROUND(AVG(fdc.option_numeric),2) AS avg_score
  //                   FROM form_data_company${company_id} AS fdc
  //                   INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
  //                   INNER JOIN client_segment_users AS csu
  //                     ON csu.segment_id = ? AND csu.company=?
  //                     AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  //                   INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
  //                   LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
  //                   LEFT JOIN category cat ON usf.category_id = cat.id
  //                   WHERE usf.company = ?
  //                   AND fdc.option_numeric REGEXP '^[0-9]+$'
  //                   /*EXTRA_FILTERS*/
  //                   /*PACK_FILTERS*/
  //                   GROUP BY fdc.wave_id, usf.subcategory_id
  //                   ORDER BY  usf.subcategory_id ASC
  //                 ),
  //               scored AS (
  //                   SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     wave_id,
  //                     avg_score AS rag_score_avg_l,
  //                     ROUND(
  //                       LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS rag_score_avg_p,
  //                     ROUND(
  //                       avg_score - LAG(avg_score) OVER (PARTITION BY subcategory_id ORDER BY wave_id ASC),
  //                       2
  //                     ) AS delta_vs_prev_avg_q
  //                   FROM base
  //               ),
  //               final AS (
  //                 SELECT
  //                     subcategory_id,
  //                     subcategory_formatted_code,
  //                     subcategory_name,
  //                     category_id,
  //                     category_name,
  //                     -- wave_id,
  //                     -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
  //                     rag_score_avg_l, -- ragscore from latest wave
  //                     rag_score_avg_p, -- ragscore from previous wave
  //                     delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
  //                   FROM scored
  //                   WHERE wave_id = ?
  //                   ORDER BY rag_score_avg_l DESC -- LIMIT 3
  //               )
  //                   SELECT
  //                     f.subcategory_id,
  //                     f.subcategory_formatted_code,
  //                     f.subcategory_name,
  //                     f.category_id,
  //                     f.category_name,
  //                     f.rag_score_avg_l,
  //                     f.rag_score_avg_p,
  //                     f.delta_vs_prev_avg,
  //                     GROUP_CONCAT(DISTINCT usf.stakeholder_id ORDER BY usf.stakeholder_id SEPARATOR ',') AS stakeholder_ids
  //                   FROM final AS f
  //                   LEFT JOIN user_stakeholder_form usf
  //                     ON usf.subcategory_id = f.subcategory_id
  //                     AND usf.company = ${company_id}
  //                   GROUP BY f.subcategory_id
  //                   ORDER BY f.rag_score_avg_l DESC
  //                   `;
  //   //console.log(baseSql);
  //   const { sql } = await applyExtraFilters(baseSql, filters);
  //   //console.log(sql);
  //   const [rows] = await pool.query(sql, [
  //     segment_id,
  //     company_id,
  //     company_id,
  //     wave_id,
  //   ]);
  //   //console.log(rows);
  //   // Process data concurrently
  //   const [enrichedRows, uniqueCategories] = await Promise.all([
  //     Promise.all(rows.map((row) => enrichWithStakeholders(row, company_id))),
  //     Promise.resolve(extractDistinctCategories(rows)),
  //   ]);
  //   // console.log(enrichedRows);
  //   // console.log(uniqueCategories);
  //   // Use results
  //   return {
  //     categories: uniqueCategories,
  //     enrichedRows: enrichedRows,
  //   };
  // }
}

/** 19) getAllCities */
export async function getAllCities(stakeholderFormId) {
  const sql = `SELECT
                      s.name as label,
                      s.id as value
                  FROM user_stakeholder_form usf
                  LEFT JOIN client_users cu
                      ON FIND_IN_SET(cu.id, usf.segments) > 0
                  WHERE usf.id = ?
                  ORDER BY cu.name ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholderFormId]);
  return rows;
}

/** 1) getAllSegments */
// export async function getAllSegments(filters) {
//   const { suffix, params } = pickVariant(filters);
//   //
//   let baseSql,
//     queryParams = [];
//   if (suffix === "portfolio") {
//     const company_id = params[0];
//     baseSql = `
//             SELECT
//                 DISTINCT(s.name) as label,
//                 s.id as value
//                 FROM form_data_company${company_id} AS fdc
//                 INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                 INNER JOIN client_segment_users AS csu
//                   ON csu.company=?
//                   AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//                 LEFT JOIN segment s ON s.id = csu.segment_id
//                 INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
//                 WHERE 1=1
//                   /*EXTRA_FILTERS*/
//                   /*PACK_FILTERS*/
//                   `;
//     queryParams = [company_id];
//   } else if (suffix === "portfolio_segment") {
//     const company_id = params[1];
//     baseSql = `
//             SELECT
//                 DISTINCT(s.name) as label,
//                 s.id as value
//                 FROM form_data_company${company_id} AS fdc
//                 INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                 INNER JOIN client_segment_users AS csu
//                   ON csu.company=?
//                   AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//                 LEFT JOIN segment s ON s.id = csu.segment_id
//                 INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
//                 WHERE 1=1
//                   /*EXTRA_FILTERS*/
//                   /*PACK_FILTERS*/
//                   `;
//     queryParams = [company_id];
//   } else if (suffix === "portfolio_wave") {
//     const wave_id = params[0];
//     const company_id = params[1];
//     baseSql = `
//             SELECT
//                 DISTINCT(s.name) as label,
//                 s.id as value
//                 FROM form_data_company${company_id} AS fdc
//                 INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                 INNER JOIN client_segment_users AS csu
//                   ON csu.company=?
//                   AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//                 LEFT JOIN segment s ON s.id = csu.segment_id
//                 INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
//                 WHERE fdc.wave_id = ?
//                   /*EXTRA_FILTERS*/
//                   /*PACK_FILTERS*/
//                   `;
//     queryParams = [company_id, wave_id];
//   } else if (suffix === "portfolio_wave_segment") {
//     const wave_id = params[0];
//     const company_id = params[2];
//     baseSql = `
//             SELECT
//                 DISTINCT(s.name) as label,
//                 s.id as value
//                 FROM form_data_company${company_id} AS fdc
//                 INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                 INNER JOIN client_segment_users AS csu
//                   ON csu.company=?
//                   AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//                 LEFT JOIN segment s ON s.id = csu.segment_id
//                 INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id
//                 WHERE fdc.wave_id = ?
//                   /*EXTRA_FILTERS*/
//                   /*PACK_FILTERS*/
//                   `;
//     queryParams = [company_id, wave_id];
//   }
//   // Apply filters
//   const { sql } = applyExtraFilters(baseSql, filters);
//   const [rows] = await pool.query(sql, queryParams);
//   //console.log(rows);
//   //console.log(rows.length);
//   return rows;
// }
