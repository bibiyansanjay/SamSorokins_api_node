// services/ragTrend.service.js
import pool from "../db";

//((((((((((((((((((((((((((((((((((((((helpers starts))))))))))))))))))))))))))))))))))))))
function pickVariant({
  stakeholder_id,
  wave_id,
  segment_id,
  company_id,
  city,
  region,
  country,
  subcategory_id,
}) {
  if (stakeholder_id && !wave_id && !segment_id && company_id)
    return {
      suffix: "stakeholder",
      params: [
        stakeholder_id,
        company_id,
        city,
        region,
        country,
        subcategory_id,
      ],
    };
  if (stakeholder_id && wave_id && !segment_id && company_id)
    return {
      suffix: "stakeholder_wave",
      params: [
        stakeholder_id,
        wave_id,
        company_id,
        city,
        region,
        country,
        subcategory_id,
      ],
    };
  if (stakeholder_id && !wave_id && segment_id && company_id)
    return {
      suffix: "stakeholder_segment",
      params: [
        stakeholder_id,
        segment_id,
        company_id,
        city,
        region,
        country,
        subcategory_id,
      ],
    };
  if (stakeholder_id && wave_id && segment_id && company_id)
    return {
      suffix: "stakeholder_wave_segment",
      params: [
        stakeholder_id,
        wave_id,
        segment_id,
        company_id,
        city,
        region,
        country,
        subcategory_id,
      ],
    };
  throw new Error("Unsupported filter combination");
}

async function applyExtraFilters(baseSql, filters, tableStr, stakeholder_id) {
  let whereParts = [];
  let subCatPart = [];
  console.log("Filters in applyExtraFilters:", filters);
  let sql = baseSql;

  if (
    filters.city &&
    typeof filters.city === "string" &&
    filters.city.length > 0
  ) {
    //console.log("Applying city filter with value:", filters.city);
    whereParts.push(`${tableStr}.city = '${filters.city}'`);
  }
  if (
    filters.region &&
    typeof filters.region === "string" &&
    filters.region.length > 0
  ) {
    whereParts.push(`${tableStr}.region = '${filters.region}'`);
  }

  if (
    filters.country &&
    typeof filters.country === "string" &&
    filters.country.length > 0
  ) {
    whereParts.push(`${tableStr}.country = '${filters.country}'`);
  }
  if (
    filters.subcategory_id &&
    typeof filters.subcategory_id === "number" &&
    filters.subcategory_id > 0
  ) {
    console.log("apply subcategory_id filter:", filters.subcategory_id);
    if (sql.indexOf("/*EXTRA_FILTERS_SUBCATEGORY*/") !== -1) {
      sql = sql.replace(
        "/*EXTRA_FILTERS_SUBCATEGORY*/",
        `AND usf.subcategory_id = '${filters.subcategory_id}'`
      );
    } else {
      sql = sql.replace("/*EXTRA_FILTERS_SUBCATEGORY*/", "");
      whereParts.push(`usf.subcategory_id = '${filters.subcategory_id}'`);
    }
  }

  // Inject extra WHERE parts before GROUP BY

  if (whereParts.length > 0) {
    //console.log("Applying extra WHERE conditions:", whereParts);
    sql = sql.replace("/*EXTRA_FILTERS*/", " AND " + whereParts.join(" AND "));
  } else {
    sql = sql.replace("/*EXTRA_FILTERS*/", "");
  }
  return { sql };
}
//((((((((((((((((((((((((((((((((((((((helpers ends))))))))))))))))))))))))))))))))))))))

//////////////////////////FORM/////////////////////////////////////////////-STARTS

/** 1) Trend (avg delta for latest/selected wave) */
export async function getTrend(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "stakeholder") {
    const stakeholder_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
    				        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                      /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id
                  ),
                scored AS (
                    SELECT
                        form_id,
                        wave_id,
                        avg_score,
                        ROUND(
                            avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                            2
                        ) AS delta_vs_prev_avg,
                         MAX(wave_id) OVER (PARTITION BY form_id) AS max_wave
                    FROM base
                )
                SELECT wave_id, avg_score, delta_vs_prev_avg 
                FROM scored
                WHERE wave_id = max_wave`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    // console.log("Trends SQL: ");
    // console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_wave") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
    				        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                      /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id
                  ),
                scored AS (
                    SELECT
                        form_id,
                        wave_id,
                        avg_score,
                        ROUND(
                            avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                            2
                        ) AS delta_vs_prev_avg
                    FROM base
                )
                SELECT form_id, wave_id, avg_score, delta_vs_prev_avg
                FROM scored
                WHERE wave_id = ?`;
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, wave_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
    				        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                      /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id
                  ),
                scored AS (
                    SELECT
                        form_id,
                        wave_id,
                        avg_score,
                        ROUND(
                            avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                            2
                        ) AS delta_vs_prev_avg,
                         MAX(wave_id) OVER (PARTITION BY form_id) AS max_wave
                    FROM base
                )
                SELECT form_id, wave_id, avg_score, delta_vs_prev_avg
                FROM scored
                WHERE wave_id = max_wave`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave_segment") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
    				        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                      /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id
                  ),
                scored AS (
                    SELECT
                        form_id,
                        wave_id,
                        avg_score,
                        ROUND(
                            avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                            2
                        ) AS delta_vs_prev_avg
                    FROM base
                )
                SELECT form_id, wave_id, avg_score, delta_vs_prev_avg
                FROM scored
                WHERE wave_id = ?`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 2-A) getRagScoreWave1 */
export async function getRagScoreWave1(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "stakeholder") {
    const stakeholder_id = params[0];
    const company_id = params[1];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
    		INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        WHERE fdc.wave_id = 1
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const stakeholder_id = params[0];
    const company_id = params[2];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        WHERE fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = 1
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.wave_id = 1
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.wave_id = 1
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 2-B) Rag Score */
export async function getRagScore(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "stakeholder") {
    const stakeholder_id = params[0];
    const company_id = params[1];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        WHERE fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        WHERE fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, wave_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave_segment") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 3) getResponseRateNResponsesCount */
export async function getResponseRateNResponsesCount(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  if (suffix === "stakeholder") {
    const stakeholder_id = params[0];
    const company_id = params[1];
    return Promise.all([
      getSentCount(stakeholder_id, filters),
      getResponsesCount(stakeholder_id, company_id, filters),
    ]).then(([sentCount, responsesCount]) => {
      console.log("sentCount:", sentCount, "responsesCount:", responsesCount);
      // Calculate percentage
      const percentage =
        sentCount > 0
          ? Math.round((responsesCount / sentCount) * 100)
          : "0"; ////console.log("percentage:", percentage.toFixed(2) + "%");
      return { percentage, sentCount, responsesCount };
    });
    async function getResponsesCount(stakeholder_id, company_id, filters) {
      const baseSql = `WITH base AS (
           SELECT
              fdc.form_id,
              fdc.wave_id,
              COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
            FROM form_data_company${company_id} AS fdc
            INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
            INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
             WHERE 1=1
             /*EXTRA_FILTERS*/
            GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
            )
            SELECT
            b.form_id,
            SUM(b.sum_wave_level_respondents) AS total_users_sum
        FROM base b
        GROUP BY b.form_id`;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [responsesRows] = await pool.query(sql, [stakeholder_id]);
      //console.log(responsesRows);
      const responsesCount = responsesRows?.[0]?.total_users_sum;
      return responsesCount;
    }
    async function getSentCount(stakeholder_id, filters) {
      const baseSql = `
                    SELECT COUNT(*) AS count_occurrences
            FROM (
                SELECT
                    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                FROM user_stakeholder_form_logs usfl
                INNER JOIN user_stakeholder_form AS usf ON usf.id = usfl.user_stakeholder_form_id AND usf.stakeholder_id = ?
                JOIN numbers
                                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                WHERE 1=1
                /*EXTRA_FILTERS_SUBCATEGORY*/
            ) split_ids
            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
            WHERE 1=1
            /*EXTRA_FILTERS*/           
            `; // -- GROUP BY cu.city
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      console.log(sql);
      const [sentRows] = await pool.query(sql, [stakeholder_id]);
      const sentCount = sentRows?.[0]?.count_occurrences;
      return sentCount;
    }
  } else if (suffix == "stakeholder_wave") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    return Promise.all([
      getSentCount(stakeholder_id, wave_id, filters),
      getResponsesCount(stakeholder_id, wave_id, company_id, filters),
    ]).then(([sentCount, responsesCount]) => {
      console.log("sentCount:", sentCount, "responsesCount:", responsesCount);
      // Calculate percentage
      const percentage =
        sentCount > 0
          ? Math.round((responsesCount / sentCount) * 100)
          : "0"; //console.log("percentage:", percentage.toFixed(2) + "%");
      return { percentage, sentCount, responsesCount };
    });
    async function getResponsesCount(
      stakeholder_id,
      wave_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH base AS (
           SELECT
              fdc.form_id,
              fdc.wave_id,
              COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
            FROM form_data_company${company_id} AS fdc
            INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
            INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
             WHERE fdc.wave_id=?
             /*EXTRA_FILTERS*/
            GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
            )
            SELECT
            b.form_id,
            SUM(b.sum_wave_level_respondents) AS total_users_sum
        FROM base b
        GROUP BY b.form_id`;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [responsesRows] = await pool.query(sql, [stakeholder_id, wave_id]);
      const responsesCount = responsesRows?.[0]?.total_users_sum;
      return responsesCount;
    }
    async function getSentCount(stakeholder_id, wave_id, filters) {
      const baseSql = `
              SELECT COUNT(*) AS count_occurrences
            FROM (
                SELECT
                    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                FROM user_stakeholder_form_logs usfl
                INNER JOIN user_stakeholder_form AS usf ON usf.id = usfl.user_stakeholder_form_id AND usf.stakeholder_id = ?
                JOIN numbers
                                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                WHERE 1=1
                /*EXTRA_FILTERS_SUBCATEGORY*/
            ) split_ids
            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
            WHERE 1=1
            /*EXTRA_FILTERS*/
            `;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [sentRows] = await pool.query(sql, [wave_id, stakeholder_id]);
      const sentCount = sentRows?.[0]?.count_occurrences;
      return sentCount;
    }
  } else if (suffix === "stakeholder_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    return Promise.all([
      getSentCount(stakeholder_id, segment_id, company_id, filters),
      getResponsesCount(stakeholder_id, segment_id, company_id, filters),
    ]).then(([sentCount, responsesCount]) => {
      console.log("sentCount:", sentCount, "responsesCount:", responsesCount);
      // Calculate percentage
      const percentage =
        sentCount > 0
          ? Math.round((responsesCount / sentCount) * 100)
          : "0"; //console.log("percentage:", percentage.toFixed(2) + "%");
      return { percentage, sentCount, responsesCount };
    });
    async function getResponsesCount(
      stakeholder_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH base AS (
           SELECT
              fdc.form_id,
              fdc.wave_id,
              COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
            FROM form_data_company${company_id} AS fdc
            INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
            INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
            INNER JOIN client_segment_users AS csu
                 ON csu.segment_id = ?            -- e.g. 23
                AND csu.company = ?               -- e.g. 2
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
             WHERE 1=1
             /*EXTRA_FILTERS*/
            GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
            )
            SELECT
            b.form_id,
            SUM(b.sum_wave_level_respondents) AS total_users_sum
        FROM base b
        GROUP BY b.form_id`;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [responsesRows] = await pool.query(sql, [
        stakeholder_id,
        segment_id,
        company_id,
      ]);
      const responsesCount = responsesRows?.[0]?.total_users_sum;
      return responsesCount;
    }
    async function getSentCount(
      stakeholder_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql1 = `
              SELECT COUNT(*) AS count_occurrences
            FROM (
                SELECT 
                    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                FROM user_stakeholder_form_logs usfl
                INNER JOIN user_stakeholder_form AS usf ON usf.id = usfl.user_stakeholder_form_id AND usf.stakeholder_id = ?
                JOIN numbers
                                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                WHERE 1=1
                /*EXTRA_FILTERS_SUBCATEGORY*/
            ) split_ids
            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
            INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?         -- e.g. 23
              AND csu.company = ?            -- e.g. 2
              AND FIND_IN_SET(split_ids.sent_id, csu.client_users) > 0
            WHERE 1=1
            /*EXTRA_FILTERS*/           
            `;
      //console.log(baseSql1);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql1, filters, tableStr);
      const [sentRows] = await pool.query(sql, [
        stakeholder_id,
        segment_id,
        company_id,
      ]);
      const sentCount = sentRows?.[0]?.count_occurrences;
      return sentCount;
    }
  } else if (suffix == "stakeholder_wave_segment") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    return Promise.all([
      getSentCount(stakeholder_id, wave_id, segment_id, company_id, filters),
      getResponsesCount(
        stakeholder_id,
        wave_id,
        segment_id,
        company_id,
        filters
      ),
    ]).then(([sentCount, responsesCount]) => {
      console.log("sentCount:", sentCount, "responsesCount:", responsesCount);
      // Calculate percentage
      const percentage =
        sentCount > 0
          ? Math.round((responsesCount / sentCount) * 100)
          : "0"; //console.log("percentage:", percentage.toFixed(2) + "%");
      return { percentage, sentCount, responsesCount };
    });
    async function getResponsesCount(
      stakeholder_id,
      wave_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH base AS (
           SELECT
              fdc.form_id,
              fdc.wave_id,
              COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
            FROM form_data_company${company_id} AS fdc
            INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
            INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
            INNER JOIN client_segment_users AS csu
                 ON csu.segment_id = ?            -- e.g. 23
                AND csu.company = ?               -- e.g. 2
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
             WHERE fdc.wave_id=?
             /*EXTRA_FILTERS*/
            GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
            )
            SELECT
            b.form_id,
            SUM(b.sum_wave_level_respondents) AS total_users_sum
        FROM base b
        GROUP BY b.form_id`;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [responsesRows] = await pool.query(sql, [
        stakeholder_id,
        segment_id,
        company_id,
        wave_id,
      ]);
      const responsesCount = responsesRows?.[0]?.total_users_sum;
      return responsesCount;
    }
    async function getSentCount(
      stakeholder_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql1 = `
                    SELECT COUNT(*) AS count_occurrences
            FROM (
                SELECT 
                    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                FROM user_stakeholder_form_logs usfl
                INNER JOIN user_stakeholder_form AS usf ON usf.id = usfl.user_stakeholder_form_id AND usf.stakeholder_id = ?
                JOIN numbers
                                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                WHERE 1=1
                /*EXTRA_FILTERS_SUBCATEGORY*/
            ) split_ids
            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
            INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?         -- e.g. 23
              AND csu.company = ?            -- e.g. 2
              AND FIND_IN_SET(split_ids.sent_id, csu.client_users) > 0
            WHERE 1=1
            /*EXTRA_FILTERS*/           
            `;
      //console.log(baseSql1);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql1, filters, tableStr);
      const [sentRows] = await pool.query(sql, [
        stakeholder_id,
        segment_id,
        company_id,
      ]);
      const sentCount = sentRows?.[0]?.count_occurrences;
      return sentCount;
    }
  }
}

/** 4) Get date range according to form n filters */
export async function getDateRange(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "stakeholder") {
    const stakeholder_id = params[0];
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
                      INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                      LEFT JOIN user_stakeholder_form_logs usfl
                        ON usfl.user_stakeholder_form_id = fdc.form_id
                      AND usfl.wave = fdc.wave_id
                      WHERE fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_segment") {
    const stakeholder_id = params[0];
    const company_id = params[2];
    const sql = `SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      CONCAT(
                          DATE_FORMAT(usfl.created_at, '%b %e, %Y'),
                          ' - ',
                          DATE_FORMAT(NOW(), '%b %e, %Y')
                      ) AS date_range
                      FROM form_data_company${company_id} AS fdc
                      INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                      LEFT JOIN user_stakeholder_form_logs usfl
                        ON usfl.user_stakeholder_form_id = fdc.form_id
                      AND usfl.wave = fdc.wave_id
                      WHERE fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_wave") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const sql = `WITH base AS (
                          SELECT
                            fdc.form_id,
                            fdc.wave_id,
                            usf.current_wave,
                            MIN(usfl.created_at) AS created_at
                          FROM form_data_company${company_id} AS fdc
                          LEFT JOIN user_stakeholder_form_logs usfl
                            ON usfl.user_stakeholder_form_id = fdc.form_id 
                          AND usfl.wave = fdc.wave_id
                          LEFT JOIN user_stakeholder_form AS usf
                            ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                          WHERE 1=1
                          GROUP BY fdc.wave_id
                          ORDER BY fdc.wave_id ASC
                        ),
                        waves AS (
                          SELECT
                            form_id,
                            wave_id,
                            created_at,
                            LEAD(created_at) OVER (PARTITION BY form_id ORDER BY wave_id) AS next_wave_date,
                            MAX(wave_id) OVER (PARTITION BY form_id) AS max_wave
                          FROM base
                        )
                        SELECT
                          w.form_id,
                          w.wave_id,
                          DATE_FORMAT(w.created_at, '%b %e, %Y') AS start_date,
                          CASE 
                            WHEN w.wave_id = w.max_wave 
                              THEN DATE_FORMAT(NOW(), '%b %e, %Y')
                            ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
                          END AS end_date,
                          CONCAT(
                            DATE_FORMAT(w.created_at, '%b %e, %Y'),
                            ' - ',
                            CASE 
                              WHEN w.wave_id = w.max_wave 
                                THEN DATE_FORMAT(NOW(), '%b %e, %Y')
                              ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
                            END
                          ) AS date_range
                        FROM waves w
                        WHERE w.wave_id = ?`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, wave_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_wave_segment") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const company_id = params[3];
    const sql = `WITH base AS (
                          SELECT
                            fdc.form_id,
                            fdc.wave_id,
                            usf.current_wave,
                            MIN(usfl.created_at) AS created_at
                          FROM form_data_company${company_id} AS fdc
                          LEFT JOIN user_stakeholder_form_logs usfl
                            ON usfl.user_stakeholder_form_id = fdc.form_id 
                          AND usfl.wave = fdc.wave_id
                          LEFT JOIN user_stakeholder_form AS usf
                            ON usf.id = fdc.form_id  AND usf.stakeholder_id = ?
                          WHERE 1=1
                          GROUP BY fdc.wave_id
                          ORDER BY fdc.wave_id ASC
                        ),
                        waves AS (
                          SELECT
                            form_id,
                            wave_id,
                            created_at,
                            LEAD(created_at) OVER (PARTITION BY form_id ORDER BY wave_id) AS next_wave_date,
                            MAX(wave_id) OVER (PARTITION BY form_id) AS max_wave
                          FROM base
                        )
                        SELECT
                          w.form_id,
                          w.wave_id,
                          DATE_FORMAT(w.created_at, '%b %e, %Y') AS start_date,
                          CASE 
                            WHEN w.wave_id = w.max_wave 
                              THEN DATE_FORMAT(NOW(), '%b %e, %Y')
                            ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
                          END AS end_date,
                          CONCAT(
                            DATE_FORMAT(w.created_at, '%b %e, %Y'),
                            ' - ',
                            CASE 
                              WHEN w.wave_id = w.max_wave 
                                THEN DATE_FORMAT(NOW(), '%b %e, %Y')
                              ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
                            END
                          ) AS date_range
                        FROM waves w
                        WHERE w.wave_id = ?`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, wave_id]);
    //console.log(rows);
    return rows;
  }
}

export async function getWavePercentages(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "stakeholder") {
    const stakeholder_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                                        /*EXTRA_FILTERS*/
                                      GROUP BY fdc.respondent_id
                                  ),
                                  ClassifiedResponses AS (
                                      SELECT
                                          wave_id,
                                          respondent_id,
                                          avg_score,
                                          CASE
                                            WHEN avg_score >= 4 THEN 'Green'
                                            WHEN avg_score >= 3 THEN 'Amber'
                                            ELSE 'Red'
                                          END AS response_category
                                      FROM AvgScores
                                  )
                                      SELECT
                                          ROUND(COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Red'   THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage
                                      FROM ClassifiedResponses
                                      ORDER BY wave_id DESC`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_wave") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE fdc.wave_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
                                        /*EXTRA_FILTERS*/
                                      GROUP BY fdc.wave_id, fdc.respondent_id
                                  ),
                                  ClassifiedResponses AS (
                                      SELECT
                                          wave_id,
                                          respondent_id,
                                          avg_score,
                                          CASE
                                            WHEN avg_score >= 4 THEN 'Green'
                                            WHEN avg_score >= 3 THEN 'Amber'
                                            ELSE 'Red'
                                          END AS response_category
                                      FROM AvgScores
                                  )
                                      SELECT
                                          wave_id,
                                          ROUND(COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Red'   THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage
                                      FROM ClassifiedResponses
                                      GROUP BY wave_id`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, wave_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                          AND csu.company = ?               -- e.g. 2
                                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                                        /*EXTRA_FILTERS*/
                                      GROUP BY fdc.respondent_id
                                  ),
                                  ClassifiedResponses AS (
                                      SELECT
                                          wave_id,
                                          respondent_id,
                                          avg_score,
                                          CASE
                                            WHEN avg_score >= 4 THEN 'Green'
                                            WHEN avg_score >= 3 THEN 'Amber'
                                            ELSE 'Red'
                                          END AS response_category
                                      FROM AvgScores
                                  )
                                      SELECT
                                          ROUND(COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Red'   THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage
                                      FROM ClassifiedResponses
                                      ORDER BY wave_id DESC`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave_segment") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                          AND csu.company = ?               -- e.g. 2
                                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE fdc.wave_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
                                        /*EXTRA_FILTERS*/
                                      GROUP BY fdc.wave_id, fdc.respondent_id
                                  ),
                                  ClassifiedResponses AS (
                                      SELECT
                                          wave_id,
                                          respondent_id,
                                          avg_score,
                                          CASE
                                            WHEN avg_score >= 4 THEN 'Green'
                                            WHEN avg_score >= 3 THEN 'Amber'
                                            ELSE 'Red'
                                          END AS response_category
                                      FROM AvgScores
                                  )
                                      SELECT
                                          wave_id,
                                          ROUND(COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Red'   THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage
                                      FROM ClassifiedResponses
                                      GROUP BY wave_id`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}
//////////////////////////FORM/////////////////////////////////////////////-ENDS

//1111111111111111111111111111
//get date range for all data
export async function part1GetDateRange(stakeholder_id, company_id) {
  const sql = `WITH base AS (
                          SELECT
                            fdc.form_id,
                            fdc.wave_id,
                            usf.current_wave,
                            MIN(usfl.created_at) AS created_at
                          FROM form_data_company${company_id} AS fdc
                          LEFT JOIN user_stakeholder_form_logs usfl
                            ON usfl.user_stakeholder_form_id = fdc.form_id 
                          AND usfl.wave = fdc.wave_id
                          LEFT JOIN user_stakeholder_form AS usf
                            ON usf.id = fdc.form_id and usf.stakeholder_id = ?
                          WHERE 1=1
                          GROUP BY fdc.wave_id
                          ORDER BY fdc.wave_id ASC
                        ),
                        waves AS (
                          SELECT
                            form_id,
                            wave_id,
                            created_at,
                            LEAD(created_at) OVER (PARTITION BY form_id ORDER BY wave_id) AS next_wave_date,
                            MAX(wave_id) OVER (PARTITION BY form_id) AS max_wave
                          FROM base
                        )
                        SELECT
                          w.form_id,
                          w.wave_id,
                          DATE_FORMAT(w.created_at, '%b %e, %Y') AS start_date,
                          CASE 
                            WHEN w.wave_id = w.max_wave 
                              THEN DATE_FORMAT(NOW(), '%b %e, %Y')
                            ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
                          END AS end_date,
                          CONCAT(
                            DATE_FORMAT(w.created_at, '%b %e, %Y'),
                            ' - ',
                            CASE 
                              WHEN w.wave_id = w.max_wave 
                                THEN DATE_FORMAT(NOW(), '%b %e, %Y')
                              ELSE DATE_FORMAT(DATE_SUB(w.next_wave_date, INTERVAL 1 DAY), '%b %e, %Y')
                            END
                          ) AS date_range
                        FROM waves w
                        ORDER BY w.wave_id`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholder_id]);
  //console.log(rows);
  return rows;
}

//333333333333333333333333333333
/** 3)part3getRagDelta */
export async function part3getRagDelta(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "stakeholder") {
    const stakeholder_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH all_waves AS (
                        SELECT DISTINCT fdc.wave_id
                        FROM form_data_company${company_id} fdc
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                        WHERE 1=1
                    ),
                    base AS (
                        SELECT
                          fdc.form_id,
                          fdc.wave_id,
                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                        FROM form_data_company${company_id} AS fdc
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                        WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                          /*EXTRA_FILTERS*/
                        GROUP BY fdc.form_id, fdc.wave_id
                    ),
                    merged AS (
                        SELECT 
                            aw.wave_id,
                            b.form_id,
                            COALESCE(b.avg_score, 0) AS avg_score
                        FROM all_waves aw
                        LEFT JOIN base b ON aw.wave_id = b.wave_id
                    ),
                    scored AS (
                        SELECT
                            form_id,
                            wave_id,
                            avg_score,
                            ROUND(
                                avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                                2
                            ) AS delta_vs_prev_avg
                        FROM merged
                    )
                    SELECT wave_id, avg_score, COALESCE(delta_vs_prev_avg, 0) AS delta_vs_prev_avg
                    FROM scored
                    ORDER BY wave_id DESC;
                    `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_wave") {
    const stakeholder_id = params[0];
    const company_id = params[2];
    const baseSql = `WITH all_waves AS (
                        SELECT DISTINCT fdc.wave_id
                        FROM form_data_company${company_id} fdc
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                        WHERE 1=1
                    ),
                    base AS (
                        SELECT
                          fdc.form_id,
                          fdc.wave_id,
                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                        FROM form_data_company${company_id} AS fdc
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                        WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                          /*EXTRA_FILTERS*/
                        GROUP BY fdc.form_id, fdc.wave_id
                    ),
                    merged AS (
                        SELECT 
                            aw.wave_id,
                            b.form_id,
                            COALESCE(b.avg_score, 0) AS avg_score
                        FROM all_waves aw
                        LEFT JOIN base b ON aw.wave_id = b.wave_id
                    ),
                    scored AS (
                        SELECT
                            form_id,
                            wave_id,
                            avg_score,
                            ROUND(
                                avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                                2
                            ) AS delta_vs_prev_avg
                        FROM merged
                    )
                    SELECT wave_id, avg_score, COALESCE(delta_vs_prev_avg, 0) AS delta_vs_prev_avg
                    FROM scored
                    ORDER BY wave_id DESC;
                    `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH all_waves AS (
                        SELECT DISTINCT fdc.wave_id
                        FROM form_data_company${company_id} fdc
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                        WHERE 1=1
                    ),
                    base AS (
                        SELECT
                          fdc.form_id,
                          fdc.wave_id,
                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                        FROM form_data_company${company_id} AS fdc
                        INNER JOIN client_segment_users AS csu
                            ON csu.segment_id = ?            -- e.g. 23
                          AND csu.company = ?               -- e.g. 2
                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                        WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                          /*EXTRA_FILTERS*/
                        GROUP BY fdc.form_id, fdc.wave_id
                    ),
                    merged AS (
                        SELECT 
                            aw.wave_id,
                            b.form_id,
                            COALESCE(b.avg_score, 0) AS avg_score
                        FROM all_waves aw
                        LEFT JOIN base b ON aw.wave_id = b.wave_id
                    ),
                    scored AS (
                        SELECT
                            form_id,
                            wave_id,
                            avg_score,
                            ROUND(
                                avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                                2
                            ) AS delta_vs_prev_avg
                        FROM merged
                    )
                    SELECT wave_id, avg_score, COALESCE(delta_vs_prev_avg, 0) AS delta_vs_prev_avg
                    FROM scored
                    ORDER BY wave_id DESC
                    `;
    //console.log(baseSql);
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
      stakeholder_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH all_waves AS (
                        SELECT DISTINCT fdc.wave_id
                        FROM form_data_company${company_id} fdc
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                        WHERE 1=1
                    ),
                    base AS (
                        SELECT
                          fdc.form_id,
                          fdc.wave_id,
                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                        FROM form_data_company${company_id} AS fdc
                        INNER JOIN client_segment_users AS csu
                            ON csu.segment_id = ?            -- e.g. 23
                          AND csu.company = ?               -- e.g. 2
                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                        WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                          /*EXTRA_FILTERS*/
                        GROUP BY fdc.form_id, fdc.wave_id
                    ),
                    merged AS (
                        SELECT 
                            aw.wave_id,
                            b.form_id,
                            COALESCE(b.avg_score, 0) AS avg_score
                        FROM all_waves aw
                        LEFT JOIN base b ON aw.wave_id = b.wave_id
                    ),
                    scored AS (
                        SELECT
                            form_id,
                            wave_id,
                            avg_score,
                            ROUND(
                                avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                                2
                            ) AS delta_vs_prev_avg
                        FROM merged
                    )
                    SELECT wave_id, avg_score, COALESCE(delta_vs_prev_avg, 0) AS delta_vs_prev_avg
                    FROM scored
                    ORDER BY wave_id DESC
                    `;
    //console.log(baseSql);
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
      stakeholder_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

//44444444444444444444444444444
export async function part4getWavePercentages(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "stakeholder") {
    const stakeholder_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH all_waves AS (
                                      SELECT DISTINCT fdc.wave_id
                                      FROM form_data_company${company_id} fdc
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE 1=1
                                  ),
                                  AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                                        /*EXTRA_FILTERS*/
                                      GROUP BY fdc.wave_id, fdc.respondent_id
                                  ),
                                  ClassifiedResponses AS (
                                      SELECT
                                          wave_id,
                                          respondent_id,
                                          avg_score,
                                          CASE
                                            WHEN avg_score >= 4 THEN 'Green'
                                            WHEN avg_score >= 3 THEN 'Amber'
                                            ELSE 'Red'
                                          END AS response_category
                                      FROM AvgScores
                                  ),
                                  WaveStats AS (
                                      SELECT
                                          wave_id,
                                          ROUND(COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Red'   THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage,
                                          ROUND(SUM(CASE WHEN avg_score < (
                                                            SELECT COALESCE(u.critical_threshold, 2.5)
                                                            FROM user u
                                                            WHERE u.id = ?
                                                        ) THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
                                          ) AS below_threshold_percentage
                                      FROM ClassifiedResponses
                                      GROUP BY wave_id
                                  )
                                  SELECT
                                      aw.wave_id,
                                      COALESCE(ws.amber_percentage, 0) AS amber_percentage,
                                      COALESCE(ws.red_percentage, 0)   AS red_percentage,
                                      COALESCE(ws.green_percentage, 0) AS green_percentage,
                                      COALESCE(ws.below_threshold_percentage, 0) AS below_threshold_percentage
                                  FROM all_waves aw
                                  LEFT JOIN WaveStats ws ON ws.wave_id = aw.wave_id
                                  ORDER BY aw.wave_id DESC`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      stakeholder_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_wave") {
    const stakeholder_id = params[0];
    const company_id = params[2];
    const baseSql = `WITH all_waves AS (
                                      SELECT DISTINCT fdc.wave_id
                                      FROM form_data_company${company_id} fdc
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE 1=1
                                  ),
                                  AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                                        /*EXTRA_FILTERS*/
                                      GROUP BY fdc.wave_id, fdc.respondent_id
                                  ),
                                  ClassifiedResponses AS (
                                      SELECT
                                          wave_id,
                                          respondent_id,
                                          avg_score,
                                          CASE
                                            WHEN avg_score >= 4 THEN 'Green'
                                            WHEN avg_score >= 3 THEN 'Amber'
                                            ELSE 'Red'
                                          END AS response_category
                                      FROM AvgScores
                                  ),
                                  WaveStats AS (
                                      SELECT
                                          wave_id,
                                          ROUND(COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Red'   THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage,
                                          ROUND(SUM(CASE WHEN avg_score < (
                                                            SELECT COALESCE(u.critical_threshold, 2.5)
                                                            FROM user u
                                                            WHERE u.id = ?
                                                        ) THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
                                          ) AS below_threshold_percentage
                                      FROM ClassifiedResponses
                                      GROUP BY wave_id
                                  )
                                  SELECT
                                      aw.wave_id,
                                      COALESCE(ws.amber_percentage, 0) AS amber_percentage,
                                      COALESCE(ws.red_percentage, 0)   AS red_percentage,
                                      COALESCE(ws.green_percentage, 0) AS green_percentage,
                                      COALESCE(ws.below_threshold_percentage, 0) AS below_threshold_percentage
                                  FROM all_waves aw
                                  LEFT JOIN WaveStats ws ON ws.wave_id = aw.wave_id
                                  ORDER BY aw.wave_id DESC`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      stakeholder_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH all_waves AS (
                                      SELECT DISTINCT fdc.wave_id
                                      FROM form_data_company${company_id} fdc
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE 1=1
                                  ),
                                  AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                          AND csu.company = ?               -- e.g. 2
                                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                                        /*EXTRA_FILTERS*/
                                      GROUP BY fdc.wave_id, fdc.respondent_id
                                  ),
                                  ClassifiedResponses AS (
                                      SELECT
                                          wave_id,
                                          respondent_id,
                                          avg_score,
                                          CASE
                                            WHEN avg_score >= 4 THEN 'Green'
                                            WHEN avg_score >= 3 THEN 'Amber'
                                            ELSE 'Red'
                                          END AS response_category
                                      FROM AvgScores
                                  ),
                                  WaveStats AS (
                                      SELECT
                                          wave_id,
                                          ROUND(COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Red'   THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage,
                                          ROUND(SUM(CASE WHEN avg_score < (
                                                            SELECT COALESCE(u.critical_threshold, 2.5)
                                                            FROM user u
                                                            WHERE u.id = ?
                                                        ) THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
                                          ) AS below_threshold_percentage
                                      FROM ClassifiedResponses
                                      GROUP BY wave_id
                                  )
                                  SELECT
                                      aw.wave_id,
                                      COALESCE(ws.amber_percentage, 0) AS amber_percentage,
                                      COALESCE(ws.red_percentage, 0)   AS red_percentage,
                                      COALESCE(ws.green_percentage, 0) AS green_percentage,
                                      COALESCE(ws.below_threshold_percentage, 0) AS below_threshold_percentage
                                  FROM all_waves aw
                                  LEFT JOIN WaveStats ws ON ws.wave_id = aw.wave_id
                                  ORDER BY aw.wave_id DESC`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
      stakeholder_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH all_waves AS (
                                      SELECT DISTINCT fdc.wave_id
                                      FROM form_data_company${company_id} fdc
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE 1=1
                                  ),
                                  AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                          AND csu.company = ?               -- e.g. 2
                                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                      WHERE fdc.option_numeric REGEXP '^[0-9]+$'
                                        /*EXTRA_FILTERS*/
                                      GROUP BY fdc.wave_id, fdc.respondent_id
                                  ),
                                  ClassifiedResponses AS (
                                      SELECT
                                          wave_id,
                                          respondent_id,
                                          avg_score,
                                          CASE
                                            WHEN avg_score >= 4 THEN 'Green'
                                            WHEN avg_score >= 3 THEN 'Amber'
                                            ELSE 'Red'
                                          END AS response_category
                                      FROM AvgScores
                                  ),
                                  WaveStats AS (
                                      SELECT
                                          wave_id,
                                          ROUND(COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Red'   THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
                                          ROUND(COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage,
                                          ROUND(SUM(CASE WHEN avg_score < (
                                                            SELECT COALESCE(u.critical_threshold, 2.5)
                                                            FROM user u
                                                            WHERE u.id = ?
                                                        ) THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
                                          ) AS below_threshold_percentage
                                      FROM ClassifiedResponses
                                      GROUP BY wave_id
                                  )
                                  SELECT
                                      aw.wave_id,
                                      COALESCE(ws.amber_percentage, 0) AS amber_percentage,
                                      COALESCE(ws.red_percentage, 0)   AS red_percentage,
                                      COALESCE(ws.green_percentage, 0) AS green_percentage,
                                      COALESCE(ws.below_threshold_percentage, 0) AS below_threshold_percentage
                                  FROM all_waves aw
                                  LEFT JOIN WaveStats ws ON ws.wave_id = aw.wave_id
                                  ORDER BY aw.wave_id DESC`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      segment_id,
      company_id,
      stakeholder_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

//55555555555555555555555555555
/** 3) getResponseRateNResponsesCount */
export async function part5getResponseRateNResponsesCount(
  filters,
  stakeholder_id
) {
  console.log("part5getResponseRateNResponsesCount:");
  const { suffix, params } = pickVariant(filters);
  if (suffix === "stakeholder") {
    const stakeholder_id = params[0];
    const company_id = params[1];
    return Promise.all([
      getSentCount(stakeholder_id, filters),
      getResponsesCount(stakeholder_id, company_id, filters),
    ]).then(([sentCount, responsesCount]) => {
      console.log("sentCount:", sentCount, "responsesCount:", responsesCount);

      // Convert responsesCount into a lookup by wave_id
      const responsesMap = Object.fromEntries(
        responsesCount.map((r) => [Number(r.wave_id), r.total_users_sum])
      );

      // Merge and calculate percentages
      const merged = sentCount.map((s) => {
        const resp = responsesMap[s.wave_id] ?? 0;
        const perc =
          s.count_occurrences > 0
            ? Math.round((resp / s.count_occurrences) * 100)
            : "0";
        return {
          wave_id: s.wave_id,
          sentCount: s.count_occurrences,
          responsesCount: resp,
          percentage: perc,
        };
      });
      return merged;
    });
    async function getResponsesCount(stakeholder_id, company_id, filters) {
      const baseSql = `WITH all_waves AS (
                                        SELECT DISTINCT fdc.wave_id
                                        FROM form_data_company${company_id} fdc
                                      INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                        WHERE 1=1
                                        /*EXTRA_FILTERS_SUBCATEGORY*/
                                    ),
                                    wave_counts AS (
                                        SELECT
                                          t.wave_id,
                                          COUNT(*) AS total_users_sum
                                        FROM (
                                          SELECT DISTINCT form_id, wave_id, respondent_id
                                          FROM form_data_company${company_id} fdc
                                          INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                          WHERE 1=1
                                          /*EXTRA_FILTERS_SUBCATEGORY*/
                                        ) t
                                        WHERE 1=1
                                          /*EXTRA_FILTERS*/
                                        GROUP BY t.wave_id
                                        ORDER BY t.wave_id
                                    )
                                    SELECT
                                        aw.wave_id,
                                        COALESCE(wc.total_users_sum, 0) AS total_users_sum
                                    FROM all_waves aw
                                    LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave_id
                                    ORDER BY aw.wave_id DESC;
                                    `;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [responsesRows] = await pool.query(sql, [
        stakeholder_id,
        stakeholder_id,
      ]);
      //console.log(responsesRows);
      return responsesRows;
    }
    async function getSentCount(stakeholder_id, filters) {
      const baseSql = `WITH split_ids AS (
                                    SELECT 
                                      usfl.user_stakeholder_form_id,
                                      usfl.wave,
                                      TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                                    FROM user_stakeholder_form_logs usfl
                                    INNER JOIN user_stakeholder_form AS usf ON usf.id = usfl.user_stakeholder_form_id AND usf.stakeholder_id = ?
                                    JOIN numbers
                                      ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                                    WHERE usfl.sent_ids IS NOT NULL AND usfl.sent_ids <> ''
                                    /*EXTRA_FILTERS_SUBCATEGORY*/
                                  ),
                                  all_waves AS (
                                    SELECT DISTINCT s.wave
                                    FROM split_ids s
                                    INNER JOIN client_users cu
                                      ON cu.id = s.sent_id
                                    AND cu.stakeholder_id = ?
                                    /* optionally: AND <other filters> */
                                  ),
                                  wave_counts AS (
                                    SELECT 
                                      s.wave AS wave_id,
                                      COUNT(*) AS count_occurrences
                                    FROM split_ids s
                                    INNER JOIN client_users cu
                                      ON cu.id = s.sent_id
                                    AND cu.stakeholder_id = ?
                                    /*EXTRA_FILTERS*/
                                    GROUP BY s.wave
                                  )
                                  SELECT 
                                    aw.wave AS wave_id,
                                    COALESCE(wc.count_occurrences, 0) AS count_occurrences
                                  FROM all_waves aw
                                  LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave
                                  ORDER BY aw.wave;
                        `;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [sentRows] = await pool.query(sql, [
        stakeholder_id,
        stakeholder_id,
        stakeholder_id,
      ]);
      return sentRows;
    }
  } else if (suffix == "stakeholder_wave") {
    const stakeholder_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    return Promise.all([
      getSentCount(stakeholder_id, filters),
      getResponsesCount(stakeholder_id, company_id, filters),
    ]).then(([sentCount, responsesCount]) => {
      console.log("sentCount:", sentCount, "responsesCount:", responsesCount);

      // Convert responsesCount into a lookup by wave_id
      const responsesMap = Object.fromEntries(
        responsesCount.map((r) => [Number(r.wave_id), r.total_users_sum])
      );

      // Merge and calculate percentages
      const merged = sentCount.map((s) => {
        const resp = responsesMap[s.wave_id] ?? 0;
        const perc =
          s.count_occurrences > 0
            ? Math.round((resp / s.count_occurrences) * 100)
            : "0";
        return {
          wave_id: s.wave_id,
          sentCount: s.count_occurrences,
          responsesCount: resp,
          percentage: perc,
        };
      });
      return merged;
    });
    async function getResponsesCount(stakeholder_id, company_id, filters) {
      const baseSql = `WITH all_waves AS (
                                        SELECT DISTINCT fdc.wave_id
                                        FROM form_data_company${company_id} fdc
                                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                        WHERE 1=1
                                        /*EXTRA_FILTERS_SUBCATEGORY*/
                                    ),
                                    wave_counts AS (
                                        SELECT
                                          t.wave_id,
                                          COUNT(*) AS total_users_sum
                                        FROM (
                                          SELECT DISTINCT form_id, wave_id, respondent_id
                                          FROM form_data_company${company_id} fdc
                                          INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                          WHERE 1=1
                                          /*EXTRA_FILTERS_SUBCATEGORY*/
                                        ) t
                                        WHERE 1=1
                                          /*EXTRA_FILTERS*/
                                        GROUP BY t.wave_id
                                        ORDER BY t.wave_id
                                    )
                                    SELECT
                                        aw.wave_id,
                                        COALESCE(wc.total_users_sum, 0) AS total_users_sum
                                    FROM all_waves aw
                                    LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave_id
                                    ORDER BY aw.wave_id DESC;
                                    `;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [responsesRows] = await pool.query(sql, [
        stakeholder_id,
        stakeholder_id,
      ]);
      //console.log(responsesRows);
      return responsesRows;
    }
    async function getSentCount(stakeholder_id, filters) {
      const baseSql = `WITH split_ids AS (
                                    SELECT 
                                      usfl.user_stakeholder_form_id,
                                      usfl.wave,
                                      TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                                    FROM user_stakeholder_form_logs usfl
                                    INNER JOIN user_stakeholder_form AS usf ON usf.id = usfl.user_stakeholder_form_id AND usf.stakeholder_id = ?
                                    JOIN numbers
                                      ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                                    WHERE usfl.sent_ids IS NOT NULL AND usfl.sent_ids <> ''
                                    /*EXTRA_FILTERS_SUBCATEGORY*/
                                  ),
                                  all_waves AS (
                                    SELECT DISTINCT s.wave
                                    FROM split_ids s
                                    INNER JOIN client_users cu
                                      ON cu.id = s.sent_id
                                    AND cu.stakeholder_id = ?
                                    /* optionally: AND <other filters> */
                                  ),
                                  wave_counts AS (
                                    SELECT 
                                      s.wave AS wave_id,
                                      COUNT(*) AS count_occurrences
                                    FROM split_ids s
                                    INNER JOIN client_users cu
                                      ON cu.id = s.sent_id
                                    AND cu.stakeholder_id = ?
                                    /*EXTRA_FILTERS*/
                                    GROUP BY s.wave
                                  )
                                  SELECT 
                                    aw.wave AS wave_id,
                                    COALESCE(wc.count_occurrences, 0) AS count_occurrences
                                  FROM all_waves aw
                                  LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave
                                  ORDER BY aw.wave;
                        `;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [sentRows] = await pool.query(sql, [
        stakeholder_id,
        stakeholder_id,
        stakeholder_id,
      ]);
      return sentRows;
    }
  } else if (suffix === "stakeholder_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    return Promise.all([
      getSentCount(stakeholder_id, segment_id, company_id, filters),
      getResponsesCount(stakeholder_id, segment_id, company_id, filters),
    ]).then(([sentCount, responsesCount]) => {
      console.log("sentCount:", sentCount, "responsesCount:", responsesCount);

      // Convert responsesCount into a lookup by wave_id
      const responsesMap = Object.fromEntries(
        responsesCount.map((r) => [Number(r.wave_id), r.total_users_sum])
      );

      // Merge and calculate percentages
      const merged = sentCount.map((s) => {
        const resp = responsesMap[s.wave_id] ?? 0;
        const perc =
          s.count_occurrences > 0
            ? Math.round((resp / s.count_occurrences) * 100)
            : "0";
        return {
          wave_id: s.wave_id,
          sentCount: s.count_occurrences,
          responsesCount: resp,
          percentage: perc,
        };
      });
      return merged;
    });
    async function getResponsesCount(
      stakeholder_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH all_waves AS (
                                        SELECT DISTINCT fdc.wave_id
                                        FROM form_data_company${company_id} fdc
                                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                        WHERE 1=1
                                        /*EXTRA_FILTERS_SUBCATEGORY*/
                                    ),
                                    wave_counts AS (
                                        SELECT
                                            fdc.form_id,
                                            fdc.wave_id,
                                            COUNT(DISTINCT fdc.respondent_id) AS total_users_sum
                                        FROM form_data_company${company_id} AS fdc
                                        INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                            AND csu.company = ?               -- e.g. 2
                                            AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                        WHERE 1=1
                                        /*EXTRA_FILTERS_SUBCATEGORY*/
                                          /*EXTRA_FILTERS*/
                                        GROUP BY fdc.wave_id
                                    )
                                    SELECT
                                        aw.wave_id,
                                        wc.form_id,
                                        COALESCE(wc.total_users_sum, 0) AS total_users_sum
                                    FROM all_waves aw
                                    LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave_id
                                    ORDER BY aw.wave_id DESC;
                                    `;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [responsesRows] = await pool.query(sql, [
        stakeholder_id,
        segment_id,
        company_id,
        stakeholder_id,
      ]);
      return responsesRows;
    }
    async function getSentCount(
      stakeholder_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH split_ids AS (
                                    SELECT 
                                      usfl.user_stakeholder_form_id,
                                      usfl.wave,
                                      TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                                    FROM user_stakeholder_form_logs usfl
                                    INNER JOIN user_stakeholder_form AS usf ON usf.id = usfl.user_stakeholder_form_id AND usf.stakeholder_id = 1
                                    JOIN numbers
                                      ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                                    WHERE usfl.sent_ids IS NOT NULL AND usfl.sent_ids <> ''
                                    /*EXTRA_FILTERS_SUBCATEGORY*/
                                  ),
                                  all_waves AS (
                                    SELECT DISTINCT s.wave
                                    FROM split_ids s
                                    INNER JOIN client_users cu
                                      ON cu.id = s.sent_id
                                    /* optionally: AND <other filters> */
                                  ),
                                  wave_counts AS (
                                    SELECT 
                                      s.wave AS wave_id,
                                      COUNT(*) AS count_occurrences
                                    FROM split_ids s
                                    INNER JOIN client_users cu
                                      ON cu.id = s.sent_id
                                      INNER JOIN client_segment_users csu
                                        ON csu.segment_id = 177         -- e.g. 23
                                      AND csu.company = 2            -- e.g. 2
                                      AND FIND_IN_SET(s.sent_id, csu.client_users) > 0                            
                                    WHERE 1=1
                                    /*EXTRA_FILTERS*/
                                    GROUP BY s.wave
                                  )
                                  SELECT 
                                    aw.wave AS wave_id,
                                    COALESCE(wc.count_occurrences, 0) AS count_occurrences
                                  FROM all_waves aw
                                  LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave
                                  ORDER BY aw.wave;
                        `;
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [sentRows] = await pool.query(sql, [
        stakeholder_id,
        stakeholder_id,
        segment_id,
        company_id,
      ]);
      return sentRows;
    }
  } else if (suffix == "stakeholder_wave_segment") {
    const stakeholder_id = params[0];
    const segment_id = params[2];
    const company_id = params[3];
    return Promise.all([
      getSentCount(stakeholder_id, segment_id, company_id, filters),
      getResponsesCount(stakeholder_id, segment_id, company_id, filters),
    ]).then(([sentCount, responsesCount]) => {
      console.log("sentCount:", sentCount, "responsesCount:", responsesCount);

      // Convert responsesCount into a lookup by wave_id
      const responsesMap = Object.fromEntries(
        responsesCount.map((r) => [Number(r.wave_id), r.total_users_sum])
      );

      // Merge and calculate percentages
      const merged = sentCount.map((s) => {
        const resp = responsesMap[s.wave_id] ?? 0;
        const perc =
          s.count_occurrences > 0
            ? Math.round((resp / s.count_occurrences) * 100)
            : "0";
        return {
          wave_id: s.wave_id,
          sentCount: s.count_occurrences,
          responsesCount: resp,
          percentage: perc,
        };
      });
      return merged;
    });
    async function getResponsesCount(
      stakeholder_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH all_waves AS (
                                        SELECT DISTINCT fdc.wave_id
                                        FROM form_data_company${company_id} fdc
                                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                        WHERE 1=1
                                        /*EXTRA_FILTERS_SUBCATEGORY*/
                                    ),
                                    wave_counts AS (
                                        SELECT
                                            fdc.form_id,
                                            fdc.wave_id,
                                            COUNT(DISTINCT fdc.respondent_id) AS total_users_sum
                                        FROM form_data_company${company_id} AS fdc
                                        INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                            AND csu.company = ?               -- e.g. 2
                                            AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                                        INNER JOIN user_stakeholder_form AS usf ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                                        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                                        WHERE 1=1
                                        /*EXTRA_FILTERS_SUBCATEGORY*/
                                          /*EXTRA_FILTERS*/
                                        GROUP BY fdc.wave_id
                                    )
                                    SELECT
                                        aw.wave_id,
                                        wc.form_id,
                                        COALESCE(wc.total_users_sum, 0) AS total_users_sum
                                    FROM all_waves aw
                                    LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave_id
                                    ORDER BY aw.wave_id DESC;
                                    `;
      //console.log(baseSql);
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [responsesRows] = await pool.query(sql, [
        stakeholder_id,
        segment_id,
        company_id,
        stakeholder_id,
      ]);
      return responsesRows;
    }
    async function getSentCount(
      stakeholder_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH split_ids AS (
                                    SELECT 
                                      usfl.user_stakeholder_form_id,
                                      usfl.wave,
                                      TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                                    FROM user_stakeholder_form_logs usfl
                                    INNER JOIN user_stakeholder_form AS usf ON usf.id = usfl.user_stakeholder_form_id AND usf.stakeholder_id = 1
                                    JOIN numbers
                                      ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                                    WHERE usfl.sent_ids IS NOT NULL AND usfl.sent_ids <> ''
                                    /*EXTRA_FILTERS_SUBCATEGORY*/
                                  ),
                                  all_waves AS (
                                    SELECT DISTINCT s.wave
                                    FROM split_ids s
                                    INNER JOIN client_users cu
                                      ON cu.id = s.sent_id
                                    /* optionally: AND <other filters> */
                                  ),
                                  wave_counts AS (
                                    SELECT 
                                      s.wave AS wave_id,
                                      COUNT(*) AS count_occurrences
                                    FROM split_ids s
                                    INNER JOIN client_users cu
                                      ON cu.id = s.sent_id
                                      INNER JOIN client_segment_users csu
                                        ON csu.segment_id = 177         -- e.g. 23
                                      AND csu.company = 2            -- e.g. 2
                                      AND FIND_IN_SET(s.sent_id, csu.client_users) > 0                            
                                    WHERE 1=1
                                    /*EXTRA_FILTERS*/
                                    GROUP BY s.wave
                                  )
                                  SELECT 
                                    aw.wave AS wave_id,
                                    COALESCE(wc.count_occurrences, 0) AS count_occurrences
                                  FROM all_waves aw
                                  LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave
                                  ORDER BY aw.wave;
                        `;
      const tableStr = `cu`;
      const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
      const [sentRows] = await pool.query(sql, [
        stakeholder_id,
        segment_id,
        company_id,
      ]);
      return sentRows;
    }
  }
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
/** 1) getFormMeta */
export async function getFormMeta(stakeholder_id) {
  const sql = `
    SELECT
      usf.company,
      usf.frequency,
      stk.name AS stakeholder_name
    FROM user_stakeholder_form usf
    LEFT JOIN stakeholder stk ON usf.stakeholder_id = stk.id
    WHERE usf.stakeholder_id = ?
    LIMIT 1
  `;
  const [rows] = await pool.query(sql, [stakeholder_id]);
  console.log(rows);
  return rows?.[0] || null;
}

/** 2) getAllSegments */
export async function getAllSegments(stakeholder_id) {
  const sql = `SELECT 
                      DISTINCT(s.name) as label,
                      s.id as value
                  FROM user_stakeholder_form usf
                  LEFT JOIN segment s 
                      ON FIND_IN_SET(s.id, usf.segments) > 0
                  WHERE usf.stakeholder_id = ?
                  ORDER BY s.name ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholder_id]);
  return rows;
}

/** 3) getAllWaves */
export async function getAllWaves(stakeholder_id) {
  const sql = `SELECT DISTINCT CONCAT("Wave ",wave_id)as label, wave_id as value FROM \`wave_question_avg\` where stakeholder_id = ? ORDER BY wave_id ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholder_id]);
  return rows;
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
