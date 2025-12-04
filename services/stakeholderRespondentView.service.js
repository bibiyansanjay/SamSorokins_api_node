// services/formView.service.js
import pool from "../db";

//((((((((((((((((((((((((((((((((((((((helpers starts))))))))))))))))))))))))))))))))))))))
function pickVariant({ respondent_id, wave_id, company_id }) {
  if (company_id && respondent_id && !wave_id)
    return { suffix: "respondent", params: [respondent_id, company_id] };
  if (company_id && respondent_id && wave_id)
    return {
      suffix: "respondent_wave",
      params: [respondent_id, wave_id, company_id],
    };
  throw new Error("Unsupported filter combination");
}
//((((((((((((((((((((((((((((((((((((((helpers ends))))))))))))))))))))))))))))))))))))))

/** 1) Get date range according to form n filters */
export async function getDateRange(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const sql = `
                SELECT
                      fdc.respondent_id,
                      usfl.created_at,
                      fdc.respondent_id,
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
                      WHERE fdc.respondent_id = ?
                      AND fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1
                `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const sql = `WITH base AS (
                          SELECT
                            fdc.respondent_id,
                            fdc.wave_id,
                            usf.current_wave,
                            MIN(usfl.created_at) AS created_at
                          FROM form_data_company${company_id} AS fdc
                          LEFT JOIN user_stakeholder_form_logs usfl
                            ON usfl.user_stakeholder_form_id = fdc.form_id 
                          AND usfl.wave = fdc.wave_id
                          INNER JOIN user_stakeholder_form AS usf
                            ON usf.id = fdc.form_id AND usf.stakeholder_id = ?
                          WHERE fdc.respondent_id = ?
                          GROUP BY fdc.respondent_id, fdc.wave_id
                          ORDER BY fdc.wave_id ASC
                        ),
                        waves AS (
                          SELECT
                            respondent_id,
                            wave_id,
                            created_at,
                            LEAD(created_at) OVER (PARTITION BY respondent_id ORDER BY wave_id) AS next_wave_date,
                            MAX(wave_id) OVER (PARTITION BY respondent_id) AS max_wave
                          FROM base
                        )
                        SELECT
                          w.respondent_id,
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
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 2) Rag Score */
export async function getRagScore(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company = params[1];
    const sql = `
              SELECT ROUND(AVG(fdc.option_numeric), 2) AS rag_score
              FROM form_data_company${company} fdc
              INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
              WHERE fdc.respondent_id=? 
                AND fdc.option_numeric REGEXP '^[0-9]+$'
            `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company = params[2];
    const sql = `
              SELECT ROUND(AVG(fdc.option_numeric), 2) AS rag_score
              FROM form_data_company${company} fdc
              INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
              WHERE fdc.respondent_id=? 
                AND fdc.option_numeric REGEXP '^[0-9]+$' 
                AND fdc.wave_id=?
            `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 3) Top 3 Scores */
export async function getTop3Scores(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.question_id,
                      fdc.wave_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.respondent_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.question_id, fdc.wave_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      respondent_id,
                      question_id,
                      wave_id,                      
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
                    GROUP BY question_id
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.question_id,
                      fdc.wave_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.respondent_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.question_id, fdc.wave_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      respondent_id,
                      question_id,
                      wave_id,                      
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
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      respondent_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 4) Bottom 3 Scores */
export async function getBottom3Scores(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.question_id,
                      fdc.wave_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.respondent_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.question_id, fdc.wave_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      respondent_id,
                      question_id,
                      wave_id,                      
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
                    GROUP BY question_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.question_id,
                      fdc.wave_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.respondent_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.question_id, fdc.wave_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      respondent_id,
                      question_id,
                      wave_id,                      
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
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      respondent_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 5) Trend Movers */
export async function getTrendMovers(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.question_id,
                      fdc.wave_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.respondent_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.question_id, fdc.wave_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      respondent_id,
                      question_id,
                      wave_id,                      
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
                ),
                final AS (
                  SELECT
                      question_id,
                      three_word_outcome_en,
                      ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this question
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
    const [rows] = await pool.query(baseSql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.question_id,
                      fdc.wave_id,
                      q.three_word_outcome_en,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.respondent_id = ?             -- e.g. 7
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.question_id, fdc.wave_id
                    ORDER BY fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      respondent_id,
                      question_id,
                      wave_id,                      
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
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      respondent_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 6) getFormsCompleted */
export async function getFormsCompleted(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company = params[1];
    const sql = `SELECT 
                    fdc.respondent_id,
                    COUNT(DISTINCT CONCAT(fdc.form_id, '-', fdc.wave_id)) AS total_submissions
                  FROM form_data_company${company} fdc
                  INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                  WHERE fdc.respondent_id = ?
                  GROUP BY fdc.respondent_id;
            `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company = params[2];
    const sql = `SELECT 
                    fdc.respondent_id,
                    COUNT(DISTINCT CONCAT(fdc.form_id, '-', fdc.wave_id)) AS total_submissions
                  FROM form_data_company${company} fdc
                  INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                  WHERE fdc.respondent_id = ? AND fdc.wave_id = ?
                  GROUP BY fdc.respondent_id;
            `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 7) getCriticalIssues */
export async function getCriticalIssues(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `
                    SELECT COUNT(*) AS below_threshold_count
                    FROM (
                      SELECT
                        fdc.question_id,
                        ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                      FROM form_data_company${company_id} AS fdc
                      INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                      WHERE fdc.respondent_id = ?
                        AND fdc.option_numeric REGEXP '^[0-9]+$'
                      GROUP BY fdc.question_id
                    ) t
                    WHERE t.avg_score < (
                                        SELECT COALESCE(u.critical_threshold, 2.5)
                                        FROM user u
                                        WHERE u.id = ?
                                        )
                `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      respondent_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `
                        SELECT COUNT(*) AS below_threshold_count
                        FROM (
                          SELECT
                            fdc.question_id,
                            ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                          FROM form_data_company${company_id} AS fdc
                          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                          WHERE fdc.respondent_id = ?
                            AND fdc.wave_id = ?   -- added wave filter
                            AND fdc.option_numeric REGEXP '^[0-9]+$'
                          GROUP BY fdc.question_id
                        ) t
                    WHERE t.avg_score < (
                                        SELECT COALESCE(u.critical_threshold, 2.5)
                                        FROM user u
                                        WHERE u.id = ?
                                        )
                `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      respondent_id,
      wave_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/* All Forms IDs */
async function getAllFormIdsNData(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company = params[1];
    const sql = `SELECT 
                      fdc.form_id, 
                      DATE_FORMAT(fdc.created_at, '%d %M %Y') AS last_activity_date,
                      usubf.category_id,
                      c.name as category_name,
                      usubf.subcategory_id,
                      s.name AS subcategory_name
                  FROM form_data_company${company} fdc
                  INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                  LEFT JOIN user_subcategory_form usubf ON usf.user_subcategory_form_id = usubf.id
                  LEFT JOIN subcategory s ON usubf.subcategory_id = s.id
                  LEFT JOIN category c ON usubf.category_id = c.id
                  WHERE fdc.respondent_id = ? 
                  GROUP BY fdc.form_id 
                  ORDER BY fdc.created_at DESC`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company = params[2];
    const sql = `SELECT 
                      fdc.form_id, 
                      DATE_FORMAT(fdc.created_at, '%d %M %Y') AS last_activity_date,
                      usubf.category_id,
                      c.name as category_name,
                      usubf.subcategory_id,
                      s.name AS subcategory_name
                  FROM form_data_company${company} fdc
                  INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                  LEFT JOIN user_subcategory_form usubf ON usf.user_subcategory_form_id = usubf.id
                  LEFT JOIN subcategory s ON usubf.subcategory_id = s.id
                  LEFT JOIN category c ON usubf.category_id = c.id
                  WHERE fdc.respondent_id = ? AND fdc.wave_id = ?
                  GROUP BY fdc.form_id 
                  ORDER BY fdc.created_at DESC`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/* All Forms */
export async function getAllForms(filters, stakeholder_id) {
  const formData = await getAllFormIdsNData(filters, stakeholder_id);
  //console.log(formData);
  const results = await Promise.all(
    formData.map(async (item) => {
      const formId = item.form_id;
      const subcategory_id = item.subcategory_id;
      const subcategory_name = item.subcategory_name;
      const last_activity_date = item.last_activity_date;
      const [qData, ragScoreWithDelta] = await Promise.all([
        getQuestionDetailsWithDelta(filters, formId),
        getRagScoreWithDelta(filters, formId),
      ]);

      return {
        form_id: formId,
        subcategory_id,
        subcategory_name,
        last_activity_date,
        qData,
        ragScoreWithDelta,
      };
    })
  );
  return results;
}

export async function getQuestionDetailsWithDelta(filters, form_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.respondent_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      fdc.answer as question_ans,
                      COUNT(*) AS answer_count,
                      -- AVG(fdc.option_numeric) AS avg_score,
                      AVG(
                            CASE
                              WHEN fdc.option_numeric REGEXP '^[0-9]+$'
                              THEN fdc.option_numeric
                              ELSE NULL
                            END
                          ) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 24
                    -- AND fdc.option_numeric REGEXP '^[0-9]+$'
                    AND fdc.respondent_id = ?
                    GROUP BY fdc.wave_id, fdc.question_id, fdc.answer
                     ),
                    scored AS (
                        SELECT
                      respondent_id,
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
                            ORDER BY COUNT(*) DESC
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
    const [rows] = await pool.query(baseSql, [form_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `
                WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.respondent_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
    				          fdc.answer as question_ans,
                      COUNT(*) AS answer_count,
                      -- AVG(fdc.option_numeric) AS avg_score,
                      AVG(
                            CASE
                              WHEN fdc.option_numeric REGEXP '^[0-9]+$'
                              THEN fdc.option_numeric
                              ELSE NULL
                            END
                          ) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 24
                    AND fdc.respondent_id = ? 
                    --  AND fdc.option_numeric REGEXP '^[0-9]+$'
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
                            ORDER BY COUNT(*) DESC
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
    const [rows] = await pool.query(baseSql, [form_id, respondent_id, wave_id]);
    //console.log(rows);
    return rows;
  }
}

/* Rag Score With Delta */
export async function getRagScoreWithDelta(filters, formId) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company = params[1];
    const sql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company} AS fdc
                    WHERE fdc.form_id = ?              -- e.g. 24
                    AND fdc.respondent_id = ?          -- e.g. 1
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.wave_id
                  ) ,
                scored AS (      
                     SELECT
                      form_id,
                      wave_id,
                      ROUND(avg_score, 2) AS rag_score_avg_q,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg_q
                    FROM base
                  ),
                scored_with_max AS (
                  SELECT
                    s.*,
                    MAX(wave_id) OVER (PARTITION BY form_id) AS max_wave_for_q
                  FROM scored s
                )
                SELECT
                      form_id,
                      ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                      -- MAX(max_wave_for_q) AS latest_wave
                    FROM scored_with_max
                    GROUP BY form_id
                `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [formId, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company = params[2];
    const sql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company} AS fdc
                    WHERE fdc.form_id = ?              -- e.g. 24
                    AND fdc.respondent_id = ?          -- e.g. 1
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.wave_id
                  ) ,
                scored AS (      
                     SELECT
                      form_id,
                      wave_id,
                      ROUND(avg_score, 2) AS rag_score_avg,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY form_id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg
                    FROM base
                  )
                SELECT * FROM scored WHERE wave_id = ?
                `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [formId, respondent_id, wave_id]);
    //console.log(rows);
    return rows;
  }
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/** 1) Respondent Meta */
export async function getRespondentMeta(clientUserId, stakeholder_id) {
  //console.log("clientUserId", clientUserId);
  const sql = `
            SELECT u.id, u.name, u.email, u.image, u.role, u.organization, u.stakeholder_id, stk.name as stakeholder_name, stk.image as stakeholder_image, stk.image_name as stakeholder_image_name, u.company,
            CONCAT(
              CASE WHEN COALESCE(u.city,'') <> '' THEN u.city ELSE '' END,
              CASE
                WHEN COALESCE(u.region,'') <> '' AND COALESCE(u.city,'') <> '' THEN CONCAT(', ', u.region)
                WHEN COALESCE(u.region,'') <> '' THEN u.region
                ELSE ''
              END,
              CASE
                WHEN COALESCE(u.country,'') <> '' AND (COALESCE(u.city,'') <> '' OR COALESCE(u.region,'') <> '') THEN CONCAT(' - ', u.country)
                WHEN COALESCE(u.country,'') <> '' THEN u.country
                ELSE ''
              END
            ) AS location
            FROM client_users u
            LEFT JOIN \`stakeholder\` stk ON u.stakeholder_id = stk.id
            WHERE u.id = ? AND u.stakeholder_id = ?
            `;
  //console.log(sql);
  const [rows] = await pool.query(sql, [clientUserId, stakeholder_id]);
  return rows[0] || {};
}

/** 3) All Waves */
export async function getAllWaves(stakeholder_id, respondent_id, company) {
  const sql = `SELECT DISTINCT CONCAT("Wave ",wave_id)as label, wave_id as value FROM \`form_data_company${company}\` fdc 
INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ? where respondent_id = ? ORDER BY wave_id ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholder_id, respondent_id]);
  return rows;
}

/** 3) All Segments */
export async function getAllSegments(stakeholder_id, respondent_id) {
  const sql = `
                  SELECT 
                         s.name as label,
                         s.id as value
                    FROM client_segment_users csu
                  LEFT JOIN segment s 
                    ON s.id=csu.segment_id
                    AND s.stakeholder_id = ?
                  WHERE FIND_IN_SET(?, csu.client_users)
                  ORDER BY s.name ASC
              `;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholder_id, respondent_id]);
  return rows;
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
