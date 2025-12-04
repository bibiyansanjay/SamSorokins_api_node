// services/formView.service.js
import pool from "../db";

//((((((((((((((((((((((((((((((((((((((helpers starts))))))))))))))))))))))))))))))))))))))
function pickVariant({
  form_id,
  wave_id,
  segment_id,
  company_id,
  city,
  region,
  country,
}) {
  // if (!form_id && !wave_id && !segment_id)
  //   return { suffix: "none", params: [] };
  if (form_id && !wave_id && !segment_id && company_id)
    return {
      suffix: "form",
      params: [form_id, company_id, city, region, country],
    };
  if (form_id && wave_id && !segment_id && company_id)
    return {
      suffix: "form_wave",
      params: [form_id, wave_id, company_id, city, region, country],
    };
  if (form_id && !wave_id && segment_id && company_id)
    return {
      suffix: "form_segment",
      params: [form_id, segment_id, company_id, city, region, country],
    };
  if (form_id && wave_id && segment_id && company_id)
    return {
      suffix: "form_wave_segment",
      params: [form_id, wave_id, segment_id, company_id, city, region, country],
    };
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

  // Inject extra WHERE parts before GROUP BY
  let sql = baseSql;
  if (whereParts.length > 0) {
    //console.log("Applying extra WHERE conditions:", whereParts);
    sql = sql.replace("/*EXTRA_FILTERS*/", " AND " + whereParts.join(" AND "));
  } else {
    sql = sql.replace("/*EXTRA_FILTERS*/", "");
  }
  return { sql };
}
//((((((((((((((((((((((((((((((((((((((helpers ends))))))))))))))))))))))))))))))))))))))

/** 2) Rag Score */
export async function getRagScore(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix == "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        WHERE fdc.form_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    queryParams = [form_id];
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        WHERE fdc.form_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    queryParams = [form_id, wave_id];
  } else if (suffix == "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id];
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id, wave_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  //console.log(sql);
  const [rows] = await pool.query(sql, queryParams);
  //console.log(rows);
  return rows;
}

/** 3) Top 3 Scores */
export async function getTop3Scores(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix == "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
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
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY form_id, question_id
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [form_id];
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
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
                  )
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    GROUP BY form_id, question_id
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [form_id, wave_id];
  } else if (suffix == "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                      
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
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
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY form_id, question_id
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id];
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                      INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                         
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
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
                  )
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    GROUP BY form_id, question_id
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id, wave_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  //console.log(sql);
  const [rows] = await pool.query(sql, queryParams);
  //console.log(rows);
  return rows;
}

/** 4) Bottom 3 Scores */
export async function getBottom3Scores(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix == "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
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
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY form_id, question_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [form_id];
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
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
                  )
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    GROUP BY form_id, question_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [form_id, wave_id];
  } else if (suffix == "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                      
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
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
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY form_id, question_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id];
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                      INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                         
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
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
                  )
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id, wave_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  //console.log(sql);
  const [rows] = await pool.query(sql, queryParams);
  //console.log(rows);
  return rows;
}

/** 5) Trend Movers */
export async function getTrendMovers(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix == "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      three_word_outcome_en,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
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
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY form_id, question_id
                  )
                    SELECT * FROM final
                     ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [form_id];
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      three_word_outcome_en,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                final AS (
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
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
    queryParams = [form_id, wave_id];
  } else if (suffix == "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      three_word_outcome_en,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
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
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY form_id, question_id
                  )
                    SELECT * FROM final
                     ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id];
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      three_word_outcome_en,
                      avg_score AS rag_score_avg_l,
                      ROUND(
                        LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS rag_score_avg_p,                     
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id ASC),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                final AS (
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
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
    queryParams = [segment_id, company_id, form_id, wave_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  //console.log(sql);
  const [rows] = await pool.query(sql, queryParams);
  //console.log(rows);
  return rows;
}

/** 6) Question details with delta_vs_prev_avg */
export async function getQuestionDetailsWithDelta(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix == "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      fdc.answer as question_ans,
                      COUNT(*) AS answer_count,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 24
                    --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id, fdc.answer
                     ),
                    scored AS (
                        SELECT
                      form_id,
                      wave_id,
                      question_id,
                      three_word_outcome_en,
                      question_ans,
                      answer_count,
                      ROUND(avg_score, 2) AS rag_score_avg_q,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                    ),
                    scored_with_max AS (
                        SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM scored s
                  ),
                  dominant_choice AS (
                      SELECT
                        form_id,
                        question_id,
                        question_ans,
                        SUM(answer_count) AS answer_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY form_id, question_id
                            ORDER BY SUM(answer_count) DESC
                        ) AS rn
                    FROM scored_with_max
                    WHERE question_ans IS NOT NULL
                    GROUP BY form_id, question_id, question_ans
                    )
                    SELECT
                      swm.form_id,
                      swm.question_id,
                      swm.three_word_outcome_en,
                      CONCAT(dc.question_ans, '(', FLOOR((dc.answer_count/SUM(swm.answer_count))*100), '%)' ) AS dominant_choice,
                      dc.answer_count AS dominant_choice_count,
                      SUM(swm.answer_count) AS answer_count,
                      ROUND(AVG(swm.rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN swm.wave_id = swm.max_wave_for_q
                              THEN swm.delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                      -- MAX(max_wave_for_q) AS latest_wave
                    FROM scored_with_max swm
                    LEFT JOIN dominant_choice dc ON dc.form_id = swm.form_id AND dc.question_id = swm.question_id AND dc.rn = 1
                    GROUP BY swm.form_id, swm.question_id
                    ORDER BY swm.question_id
    `;
    //console.log(baseSql);
    queryParams = [form_id];
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `
                WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
    				          fdc.answer as question_ans,
                      COUNT(*) AS answer_count,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 24
                    --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id, fdc.answer
                ),  
                  dominant_choice AS (
                      SELECT
                        form_id,
                      	wave_id,
                        question_id,
                        question_ans,
                        SUM(answer_count) AS answer_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY form_id, wave_id, question_id
                            ORDER BY SUM(answer_count) DESC
                        ) AS rn
                    FROM base
                    WHERE question_ans IS NOT NULL
                    GROUP BY form_id, wave_id, question_id, question_ans
                    ),
                scored AS (
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      answer_count,
                      three_word_outcome_en,
                      ROUND(avg_score, 2) AS rag_score_avg,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg
                    FROM base
                  )
                    SELECT
                      s.form_id,
                      s.wave_id,
                      s.question_id,
                      s.three_word_outcome_en,
                      s.rag_score_avg,
                      s.delta_vs_prev_avg,
                    	CONCAT(dc.question_ans, '(', FLOOR((dc.answer_count/SUM(s.answer_count))*100), '%)' ) AS dominant_choice,
                        dc.answer_count AS dominant_choice_count,
                        SUM(s.answer_count) AS answer_count
                FROM scored s
                	LEFT JOIN dominant_choice dc ON dc.form_id = s.form_id AND dc.wave_id = s.wave_id AND dc.question_id = s.question_id AND dc.rn = 1
                WHERE s.wave_id = ?
                GROUP BY s.form_id, s.wave_id, s.question_id
                ORDER BY s.question_id;
                `;
    //console.log(baseSql);
    queryParams = [form_id, wave_id];
  } else if (suffix == "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    baseSql = `
    WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      fdc.answer as question_ans,
                      COUNT(*) AS answer_count,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 24
                    --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id, fdc.answer
                     ),
                    scored AS (
                        SELECT
                      form_id,
                      wave_id,
                      question_id,
                      three_word_outcome_en,
                      question_ans,
                      answer_count,
                      ROUND(avg_score, 2) AS rag_score_avg_q,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                    ),
                    scored_with_max AS (
                        SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM scored s
                  ),
                  dominant_choice AS (
                      SELECT
                        form_id,
                        question_id,
                        question_ans,
                        SUM(answer_count) AS answer_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY form_id, question_id
                            ORDER BY SUM(answer_count) DESC
                        ) AS rn
                    FROM scored_with_max
                    WHERE question_ans IS NOT NULL
                    GROUP BY form_id, question_id, question_ans
                    )
                    SELECT
                      swm.form_id,
                      swm.question_id,
                      swm.three_word_outcome_en,
                      CONCAT(dc.question_ans, '(', FLOOR((dc.answer_count/SUM(swm.answer_count))*100), '%)' ) AS dominant_choice,
                      dc.answer_count AS dominant_choice_count,
                      SUM(swm.answer_count) AS answer_count,
                      ROUND(AVG(swm.rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN swm.wave_id = swm.max_wave_for_q
                              THEN swm.delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                      -- MAX(max_wave_for_q) AS latest_wave
                    FROM scored_with_max swm
                    LEFT JOIN dominant_choice dc ON dc.form_id = swm.form_id AND dc.question_id = swm.question_id AND dc.rn = 1
                    GROUP BY swm.form_id, swm.question_id
                    ORDER BY swm.question_id
    `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id];
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    baseSql = `
                WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
            				  fdc.answer as question_ans,
                      COUNT(*) AS answer_count,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 24
                    --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id, fdc.answer
                    ),
                  dominant_choice AS (
                      SELECT
                        form_id,
                      	wave_id,
                        question_id,
                        question_ans,
                        SUM(answer_count) AS answer_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY form_id, wave_id, question_id
                            ORDER BY SUM(answer_count) DESC
                        ) AS rn
                    FROM base
                    WHERE question_ans IS NOT NULL
                    GROUP BY form_id, wave_id, question_id, question_ans
                    ),
                scored AS (
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      answer_count,
                      three_word_outcome_en,
                      ROUND(avg_score, 2) AS rag_score_avg,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg
                    FROM base
                  )
                    SELECT
                      s.form_id,
                      s.wave_id,
                      s.question_id,
                      s.three_word_outcome_en,
                      s.rag_score_avg,
                      s.delta_vs_prev_avg,
                    	CONCAT(dc.question_ans, '(', FLOOR((dc.answer_count/SUM(s.answer_count))*100), '%)' ) AS dominant_choice,
                        dc.answer_count AS dominant_choice_count,
                        SUM(s.answer_count) AS answer_count
                FROM scored s
                	LEFT JOIN dominant_choice dc ON dc.form_id = s.form_id AND dc.wave_id = s.wave_id AND dc.question_id = s.question_id AND dc.rn = 1
                WHERE s.wave_id = ?
                GROUP BY s.form_id, s.wave_id, s.question_id
                ORDER BY s.question_id;
                `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id, wave_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  // console.log(sql);
  const [rows] = await pool.query(sql, queryParams);
  //console.log(rows);
  return rows;
}

/** 7) getdominantChoice Questions details with delta_vs_prev_avg for single select questions */
export async function getdominantChoice(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix == "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      fdc.answer as question_ans,
                      AVG(fdc.option_numeric) AS avg_score,
                      COUNT(*) AS answer_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY form_id, wave_id, question_id
                            ORDER BY COUNT(*) DESC
                        ) AS rn
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 24
                     AND q.question_type = "10" -- only single select questions allowed
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id, fdc.answer
                     ),
                     scored_with_max AS (
                        SELECT
                    *,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM base
                         ),
                         dominant_choice AS (
                        SELECT
                         form_id,
                      	wave_id,
                        question_id,
                        SUM(answer_count) AS total_answers, 
                        max_wave_for_q
                    FROM scored_with_max
                    WHERE question_ans IS NOT NULL
                    GROUP BY form_id, wave_id, question_id
                    ORDER BY wave_id
                             ),
                    all_dominant_data AS (
                        SELECT
                      s.form_id,
                      s.wave_id,
                      s.question_id,
                      s.three_word_outcome_en,
                      CONCAT(FLOOR((s.answer_count/dc.total_answers)*100), '%' ) AS dominant_choice_percent,
                      s.question_ans AS dominant_choice,
                        s.max_wave_for_q
                FROM scored_with_max s
                	INNER JOIN dominant_choice dc ON dc.form_id = s.form_id AND dc.wave_id = s.wave_id AND dc.question_id = s.question_id AND s.rn = 1
                GROUP BY s.form_id, s.wave_id, s.question_id
                ORDER BY s.wave_id DESC
                    )
                      SELECT
                      form_id,
                      question_id,
                      three_word_outcome_en,
                      MAX(CASE WHEN wave_id = max_wave_for_q THEN dominant_choice_percent END)  AS dominant_choice_percent_l,
                      MAX(CASE WHEN wave_id = max_wave_for_q - 1 THEN dominant_choice_percent END)  AS dominant_choice_percent_p,
                      dominant_choice
                FROM all_dominant_data
                GROUP BY form_id, question_id
                ORDER BY dominant_choice_percent DESC LIMIT 3;
    `;
    //console.log(baseSql);
    queryParams = [form_id];
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `
                WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
    				          fdc.answer as question_ans,
                      AVG(fdc.option_numeric) AS avg_score,
    				          COUNT(*) AS answer_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY form_id, wave_id, question_id
                            ORDER BY COUNT(*) DESC
                        ) AS rn
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 24
                      AND q.question_type = "10" -- only single select questions allowed
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id, fdc.answer
                  ),
                  dominant_choice AS (
                        SELECT
                     form_id,
                      	wave_id,
                        question_id,
                        SUM(answer_count) AS total_answers
                    FROM base
                    WHERE question_ans IS NOT NULL
                    GROUP BY form_id, wave_id, question_id
                    ),
                  all_dominant_data AS (
                    SELECT
                      s.form_id,
                      s.wave_id,
                      s.question_id,
                      s.three_word_outcome_en,
                      CONCAT(FLOOR((s.answer_count/dc.total_answers)*100), '%' ) AS dominant_choice_percent,
                      s.question_ans AS dominant_choice
                FROM base s
                	INNER JOIN dominant_choice dc ON dc.form_id = s.form_id AND dc.wave_id = s.wave_id AND dc.question_id = s.question_id AND s.rn = 1
                GROUP BY s.form_id, s.wave_id, s.question_id
                ORDER BY s.wave_id DESC
                      )
                      SELECT
                      form_id,
                      question_id,
                      three_word_outcome_en,
                      MAX(CASE WHEN wave_id = ? THEN dominant_choice_percent END)  AS dominant_choice_percent_l,
                      MAX(CASE WHEN wave_id = ? - 1 THEN dominant_choice_percent END)  AS dominant_choice_percent_p,
                      MAX(CASE WHEN wave_id = ? THEN dominant_choice END)  AS dominant_choice
                FROM all_dominant_data
                GROUP BY form_id, question_id
                ORDER BY dominant_choice_percent DESC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [form_id, wave_id, wave_id, wave_id];
  } else if (suffix == "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    baseSql = `
    WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      fdc.answer as question_ans,
                      COUNT(*) AS answer_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY form_id, wave_id, question_id
                            ORDER BY COUNT(*) DESC
                        ) AS rn
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 24
                    --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                     AND q.question_type = "10" -- only single select questions allowed
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id, fdc.answer
                     ),
                    scored_with_max AS (
                        SELECT
                    *,
                    MAX(wave_id) OVER (PARTITION BY question_id) AS max_wave_for_q
                  FROM base
                  ),
                  dominant_choice AS (
                        SELECT
                         form_id,
                      	wave_id,
                        question_id,
                        SUM(answer_count) AS total_answers, 
                        max_wave_for_q
                    FROM scored_with_max
                    WHERE question_ans IS NOT NULL
                    GROUP BY form_id, wave_id, question_id
                    ORDER BY wave_id
                    ),
                    all_dominant_data AS (
                        SELECT
                      s.form_id,
                      s.wave_id,
                      s.question_id,
                      s.three_word_outcome_en,
                      CONCAT(FLOOR((s.answer_count/dc.total_answers)*100), '%' ) AS dominant_choice_percent,
                      s.question_ans AS dominant_choice,
                        s.max_wave_for_q
                FROM scored_with_max s
                	INNER JOIN dominant_choice dc ON dc.form_id = s.form_id AND dc.wave_id = s.wave_id AND dc.question_id = s.question_id AND s.rn = 1
                GROUP BY s.form_id, s.wave_id, s.question_id
                ORDER BY s.wave_id DESC
                    )
                      SELECT
                      form_id,
                      question_id,
                      three_word_outcome_en,
                      MAX(CASE WHEN wave_id = max_wave_for_q THEN dominant_choice_percent END)  AS dominant_choice_percent_l,
                      MAX(CASE WHEN wave_id = max_wave_for_q - 1 THEN dominant_choice_percent END)  AS dominant_choice_percent_p,
                      dominant_choice
                FROM all_dominant_data
                GROUP BY form_id, question_id
                ORDER BY dominant_choice_percent DESC LIMIT 3;
    `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id];
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    baseSql = `
                WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      fdc.answer as question_ans,
                      AVG(fdc.option_numeric) AS avg_score,
                      COUNT(*) AS answer_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY form_id, wave_id, question_id
                            ORDER BY COUNT(*) DESC
                        ) AS rn
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 24
                     AND q.question_type = "10" -- only single select questions allowed
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id, fdc.answer
                    ),
                  dominant_choice AS (
                      SELECT
                        form_id,
                      	wave_id,
                        question_id,
                        SUM(answer_count) AS total_answers
                    FROM base
                    WHERE question_ans IS NOT NULL
                    GROUP BY form_id, wave_id, question_id
                    ),
                all_dominant_data AS (
                     SELECT
                      s.form_id,
                      s.wave_id,
                      s.question_id,
                      s.three_word_outcome_en,
                      CONCAT(FLOOR((s.answer_count/dc.total_answers)*100), '%' ) AS dominant_choice_percent,
                      s.question_ans AS dominant_choice
                FROM base s
                	INNER JOIN dominant_choice dc ON dc.form_id = s.form_id AND dc.wave_id = s.wave_id AND dc.question_id = s.question_id AND s.rn = 1
                GROUP BY s.form_id, s.wave_id, s.question_id
                ORDER BY s.wave_id DESC
                  )
                    SELECT
                      form_id,
                      question_id,
                      three_word_outcome_en,
                      MAX(CASE WHEN wave_id = ? THEN dominant_choice_percent END)  AS dominant_choice_percent_l,
                      MAX(CASE WHEN wave_id = ? - 1 THEN dominant_choice_percent END)  AS dominant_choice_percent_p,
                      MAX(CASE WHEN wave_id = ? THEN dominant_choice END)  AS dominant_choice
                FROM all_dominant_data
                GROUP BY form_id, question_id
                ORDER BY dominant_choice_percent DESC LIMIT 3
                `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id, wave_id, wave_id, wave_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  //console.log(sql);
  const [rows] = await pool.query(sql, queryParams);
  //console.log(rows);
  return rows;
}

/** 8) Trend (avg delta for latest/selected wave) */
export async function getTrend(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix === "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 7
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    queryParams = [form_id];
  } else if (suffix === "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 7
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    queryParams = [form_id, wave_id];
  } else if (suffix === "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 7
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    queryParams = [segment_id, company_id, form_id];
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 7
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    queryParams = [segment_id, company_id, form_id, wave_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  //console.log(sql);
  const [rows] = await pool.query(sql, queryParams);
  //console.log(rows);
  return rows;
}

/** 9) Respondents Data (dynamic table name via company lookup) */
export async function getRespondentsData(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix === "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `
    WITH base AS ( SELECT 
                        fdc.respondent_id AS id, 
                        fdc.wave_id,
                        cu.name, 
                        ROUND(AVG(fdc.option_numeric),2) AS avg_score,
                        cu.city,
                        fdc.created_at
                      FROM form_data_company${company_id} AS fdc
                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                      WHERE fdc.form_id = ?
                        AND fdc.option_numeric REGEXP '^[0-9]+$'
                        /*EXTRA_FILTERS*/
                      GROUP BY fdc.wave_id, fdc.respondent_id
                      ORDER BY fdc.respondent_id ASC, wave_id ASC
                ),
                scored AS (      
                    SELECT
					          id,
                    wave_id,
                    name,
                    avg_score,
                    city,
                    created_at,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      id,
                    name,
                    city,
                      ROUND(AVG(avg_score), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg,           -- delta from latest wave
                      -- MAX(CASE WHEN wave_id = max_wave_for_q THEN city END) AS city,                                       -- city from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN DATE_FORMAT(created_at, '%d %b %Y') END) AS created_at   -- created_at from latest wave                
                      -- MAX(max_wave_for_q) AS latest_wave
                    FROM scored_with_max
                    GROUP BY id
                    ORDER BY id ASC
    `;
    //console.log(baseSql);
    queryParams = [form_id];
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `
    WITH base AS ( SELECT 
                        fdc.respondent_id AS id, 
                        fdc.wave_id,
                        cu.name, 
                        ROUND(AVG(fdc.option_numeric),2) AS avg_score,
                        cu.city,
                        fdc.created_at
                      FROM form_data_company${company_id} AS fdc
                      INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                      WHERE fdc.form_id = ?
                        AND fdc.option_numeric REGEXP '^[0-9]+$'
                        /*EXTRA_FILTERS*/
                      GROUP BY fdc.wave_id, fdc.respondent_id
                      ORDER BY fdc.respondent_id ASC, wave_id ASC
                ),
                scored AS (      
                    SELECT
					          id,
                    wave_id,
                    name,
                    DATE_FORMAT(created_at, '%d %b %Y') AS created_at,
                    city,
                    ROUND(avg_score, 2) AS rag_score_avg,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg
                    FROM base
                )
                SELECT * FROM scored WHERE wave_id = ?    
                ORDER BY id ASC
    `;
    //console.log(baseSql);
    queryParams = [form_id, wave_id];
  } else if (suffix === "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
                    SELECT
                        fdc.respondent_id AS id, 
                        fdc.wave_id,
                        cu.name, 
                        ROUND(AVG(fdc.option_numeric),2) AS avg_score,
                        cu.city,
                        fdc.created_at
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                      GROUP BY fdc.wave_id, fdc.respondent_id
                      ORDER BY fdc.respondent_id ASC, wave_id ASC
                  ),
                scored AS (      
                    SELECT
					          id,
                    wave_id,
                    name,
                    avg_score,
                    city,
                    created_at,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY id) AS max_wave_for_q
                  FROM scored s
                )
                  SELECT
                      id,
                    name,
                    city,
                      ROUND(AVG(avg_score), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg,   -- delta from latest wave
                      -- MAX(CASE WHEN wave_id = max_wave_for_q THEN city END) AS city,                                       -- city from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN DATE_FORMAT(created_at, '%d %b %Y') END) AS created_at   -- created_at from latest wave        
                      -- MAX(max_wave_for_q) AS latest_wave
                    FROM scored_with_max
                    GROUP BY id
                    ORDER BY id ASC
                `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id];
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    baseSql = `WITH base AS (
                    SELECT
                        fdc.respondent_id AS id, 
                        fdc.wave_id,
                        cu.name, 
                        ROUND(AVG(fdc.option_numeric),2) AS avg_score,
                        cu.city,
                        fdc.created_at
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                      GROUP BY fdc.wave_id, fdc.respondent_id
                      ORDER BY fdc.respondent_id ASC, wave_id ASC
                  ),
                scored AS (      
                    SELECT
					          id,
                    wave_id,
                    name,
                    DATE_FORMAT(created_at, '%d %b %Y') AS created_at,
                    city,
                    ROUND(avg_score, 2) AS rag_score_avg,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg
                    FROM base
                  )                
                    SELECT * FROM scored WHERE wave_id = ?    
                  ORDER BY id ASC
                `;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id, wave_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  //console.log(sql);
  const [rows] = await pool.query(sql, queryParams);
  //console.log(rows);
  return { result: rows };
}

/** 10) Total Responses acc to filters */
export async function getResponseCount(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix === "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `SELECT
            fdc.form_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
           WHERE fdc.form_id = ?
           /*EXTRA_FILTERS*/`;
    //console.log(baseSql);
    queryParams = [form_id];
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `SELECT
            fdc.form_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
          WHERE fdc.form_id = ? AND fdc.wave_id=?
          /*EXTRA_FILTERS*/
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC`;
    //console.log(baseSql);
    queryParams = [form_id, wave_id];
  } else if (suffix === "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    baseSql = `SELECT
            fdc.form_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
            INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
          WHERE fdc.form_id = ?
          /*EXTRA_FILTERS*/`;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id];
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    baseSql = `SELECT
            fdc.form_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
            INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
          WHERE fdc.form_id = ? AND fdc.wave_id=?
          /*EXTRA_FILTERS*/
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC`;
    //console.log(baseSql);
    queryParams = [segment_id, company_id, form_id, wave_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  //console.log(sql);
  const [responsesRows] = await pool.query(sql, queryParams);
  const responseCount = responsesRows?.[0]?.total_users_sum || 0;
  return { responseCount };
}

/** 11) getSentCount */
export async function getSentCount(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix === "form") {
    const form_id = params[0];
    baseSql = `SELECT COUNT(DISTINCT sent_id) AS count_occurrences
              FROM (
                  SELECT
                    usfl.wave,
                    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                  FROM user_stakeholder_form_logs usfl
                  JOIN numbers
                    ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                  WHERE usfl.user_stakeholder_form_id =?
              ) split_ids
              INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
              WHERE 1=1
            /*EXTRA_FILTERS*/           
            `;
    //console.log(baseSql);
    queryParams = [form_id];
  } else if (suffix === "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    baseSql = `
           SELECT COUNT(*) AS count_occurrences
            FROM (
                SELECT
                  usfl.wave,
                  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                FROM user_stakeholder_form_logs usfl
                JOIN numbers
                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                WHERE usfl.user_stakeholder_form_id =? AND usfl.wave=?
            ) split_ids
            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
            WHERE 1=1
            /*EXTRA_FILTERS*/           
            `;
    //console.log(baseSql);
    queryParams = [form_id, wave_id];
  } else if (suffix === "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    baseSql = `
                  SELECT COUNT(DISTINCT sent_id) AS count_occurrences
                FROM (
                    SELECT
                      usfl.wave,
                      TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                    FROM user_stakeholder_form_logs usfl
                    JOIN numbers
                      ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                    WHERE usfl.user_stakeholder_form_id =?
                ) split_ids
                INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
                INNER JOIN client_segment_users AS csu
                    ON csu.segment_id = ?         -- e.g. 23
                  AND FIND_IN_SET(split_ids.sent_id, csu.client_users) > 0
                WHERE 1=1
                /*EXTRA_FILTERS*/           
                `;
    //console.log(baseSql);
    queryParams = [form_id, segment_id];
  } else if (suffix === "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    baseSql = `
              SELECT COUNT(*) AS count_occurrences
            FROM (
                SELECT
                  usfl.wave,
                  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                FROM user_stakeholder_form_logs usfl
                JOIN numbers
                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                WHERE usfl.user_stakeholder_form_id =? AND usfl.wave=?
            ) split_ids
            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
            INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?         -- e.g. 23
              AND FIND_IN_SET(split_ids.sent_id, csu.client_users) > 0
            WHERE 1=1
            /*EXTRA_FILTERS*/           
            `;
    //console.log(baseSql);
    queryParams = [form_id, wave_id, segment_id];
  }
  const { sql } = applyExtraFilters(baseSql, filters);
  const [sentRows] = await pool.query(sql, queryParams);
  const sentCount = sentRows?.[0]?.count_occurrences || 0;
  //console.log("sentCount", sentCount);
  return sentCount;
}

/** 12) Get date range according to form n filters */
export async function getDateRange(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let baseSql,
    queryParams = [];
  if (suffix === "form") {
    const form_id = params[0];
    const company_id = params[1];
    baseSql = `SELECT
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
                      WHERE fdc.form_id = ?  
                        AND fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    queryParams = [form_id];
  } else if (suffix === "form_segment") {
    const form_id = params[0];
    const company_id = params[2];
    baseSql = `SELECT
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
                      WHERE fdc.form_id = ?  
                        AND fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    queryParams = [form_id];
  } else if (suffix === "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    baseSql = `WITH base AS (
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
                            ON usf.id = fdc.form_id
                          WHERE fdc.form_id = ?
                          GROUP BY fdc.form_id, fdc.wave_id
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
    queryParams = [form_id, wave_id];
  } else if (suffix === "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const company_id = params[3];
    baseSql = `WITH base AS (
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
                            ON usf.id = fdc.form_id
                          WHERE fdc.form_id = ?
                          GROUP BY fdc.form_id, fdc.wave_id
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
    queryParams = [form_id, wave_id];
  }
  const [rows] = await pool.query(baseSql, queryParams);
  //console.log(rows);
  return rows;
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
/** 1) getAllSegments */
export async function getAllSegments(stakeholderFormId, filters) {
  const baseSql = `SELECT
                      DISTINCT s.name AS label,
                      s.id AS value
                  FROM user_stakeholder_form usf
                  LEFT JOIN segment s
                      ON FIND_IN_SET(s.id, usf.segments) > 0

                  INNER JOIN (
                      SELECT DISTINCT
                          TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id,
                      usfl.user_stakeholder_form_id AS form_id
                      FROM user_stakeholder_form_logs usfl
                      JOIN numbers
                          ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                      WHERE usfl.user_stakeholder_form_id = ?
                  ) AS split_ids
                      ON split_ids.form_id = usf.id

                  INNER JOIN client_users AS cu
                      ON cu.id = split_ids.sent_id
                  WHERE usf.id = ?
                  /*EXTRA_FILTERS*/
                  ORDER BY s.name ASC`;

  const { sql } = applyExtraFilters(baseSql, filters);

  // console.log(sql);
  const [rows] = await pool.query(sql, [stakeholderFormId, stakeholderFormId]);
  // console.log(rows);
  return rows;
}

/** 2) getAllWaves */
export async function getAllWaves(stakeholderFormId) {
  const sql = `SELECT DISTINCT CONCAT("Wave ",wave_id)as label, wave_id as value FROM \`wave_question_avg\` where form_id = ? ORDER BY wave_id ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholderFormId]);
  return rows;
}

/** 3) Subcategory + Stakeholder data */
export async function getSubcategoryAndStakeholderData(stakeholderFormId) {
  const sql = `
    SELECT sub.id as subcategory_id,
    cat.id as category_id,
    cat.name as category_name,
    CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code,
    sub.name as subcategory_name,
    sub.image as subcategory_image,
    sub.image_name as subcategory_image_name,
    sub.frequency as subcategory_frequency,
    sub.focus_area as subcategory_focus_area,
    sub.primary_objective as subcategory_primary_objective,
    stk.id as stakeholder_id,
    stk.name as stakeholder_name, 
    stk.classification as stakeholder_classification,
    stk.image as stakeholder_image,
    stk.image_name as stakeholder_image_name
    FROM wave_question_avg wqa 
    LEFT JOIN subcategory sub ON wqa.subcategory_id=sub.id 
    LEFT JOIN stakeholder stk ON wqa.stakeholder_id=stk.id 
    LEFT JOIN category cat ON wqa.category_id = cat.id
    WHERE wqa.form_id = ? 
    LIMIT 1`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholderFormId]);
  return rows;
}

/** 4) Total Responses For the form wdout filters */
export async function getTotalResponses(form_id, company_id) {
  const sql = `WITH base AS (
    		SELECT
            fdc.form_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
          FROM form_data_company${company_id} AS fdc
           WHERE fdc.form_id = ?
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
           )
           SELECT form_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`;
  //console.log(sql);
  const [responsesRows] = await pool.query(sql, [form_id]);
  const totalResponses = responsesRows?.[0]?.total_users_sum || 0;
  return { totalResponses };
}
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/** 0) getAllCities */
// export async function getAllCities(stakeholderFormId) {
//   const sql = `SELECT
//                       s.name as label,
//                       s.id as value
//                   FROM user_stakeholder_form usf
//                   LEFT JOIN client_users cu
//                       ON FIND_IN_SET(cu.id, usf.segments) > 0
//                   WHERE usf.id = ?
//                   ORDER BY cu.name ASC`;
//   //console.log(sql);
//   const [rows] = await pool.query(sql, [stakeholderFormId]);
//   return rows;
// }
