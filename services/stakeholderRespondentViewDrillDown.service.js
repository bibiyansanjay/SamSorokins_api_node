// services/ragTrend.service.js
import pool from "../db";

//((((((((((((((((((((((((((((((((((((((helpers starts))))))))))))))))))))))))))))))))))))))
function pickVariant({ respondent_id, wave_id, segment_id, company_id }) {
  if (respondent_id && !wave_id && !segment_id && company_id)
    return {
      suffix: "respondent",
      params: [respondent_id, company_id],
    };
  if (respondent_id && wave_id && !segment_id && company_id)
    return {
      suffix: "respondent_wave",
      params: [respondent_id, wave_id, company_id],
    };
  if (respondent_id && !wave_id && segment_id && company_id)
    return {
      suffix: "respondent_segment",
      params: [respondent_id, segment_id, company_id],
    };
  if (respondent_id && wave_id && segment_id && company_id)
    return {
      suffix: "respondent_wave_segment",
      params: [respondent_id, wave_id, segment_id, company_id],
    };
  throw new Error("Unsupported filter combination");
}

//((((((((((((((((((((((((((((((((((((((helpers ends))))))))))))))))))))))))))))))))))))))

///////////////////////////////////////////////////////////////////////-STARTS

/** 1) Trend (avg delta for latest/selected wave) */
export async function getTrend(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.respondent_id = ?              -- e.g. 7
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.wave_id
                  ),
                scored AS (
                    SELECT
                        respondent_id,
                        wave_id,
                        avg_score,
                        ROUND(
                            avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id),
                            2
                        ) AS delta_vs_prev_avg,
                         MAX(wave_id) OVER (PARTITION BY respondent_id) AS max_wave
                    FROM base
                )
                SELECT wave_id, avg_score, delta_vs_prev_avg -- respondent_id 
                FROM scored
                WHERE wave_id = max_wave`;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.respondent_id = ?              -- e.g. 7
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.wave_id
                  ),
                scored AS (
                    SELECT
                        respondent_id,
                        wave_id,
                        avg_score,
                        ROUND(
                            avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id),
                            2
                        ) AS delta_vs_prev_avg
                    FROM base
                )
                SELECT respondent_id, wave_id, avg_score, delta_vs_prev_avg
                FROM scored
                WHERE wave_id = ?`;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_segment") {
    const respondent_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.respondent_id = ?              -- e.g. 7
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.wave_id
                  ),
                scored AS (
                    SELECT
                        respondent_id,
                        wave_id,
                        avg_score,
                        ROUND(
                            avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id),
                            2
                        ) AS delta_vs_prev_avg,
                         MAX(wave_id) OVER (PARTITION BY respondent_id) AS max_wave
                    FROM base
                )
                SELECT respondent_id, wave_id, avg_score, delta_vs_prev_avg
                FROM scored
                WHERE wave_id = max_wave`;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      segment_id,
      company_id,
      respondent_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave_segment") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.respondent_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.respondent_id = ?              -- e.g. 7
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.wave_id
                  ),
                scored AS (
                    SELECT
                        respondent_id,
                        wave_id,
                        avg_score,
                        ROUND(
                            avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id),
                            2
                        ) AS delta_vs_prev_avg
                    FROM base
                )
                SELECT respondent_id, wave_id, avg_score, delta_vs_prev_avg
                FROM scored
                WHERE wave_id = ?`;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      segment_id,
      company_id,
      respondent_id,
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
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
        WHERE fdc.respondent_id = ?  
          AND fdc.wave_id = 1
          AND fdc.option_numeric REGEXP '^[0-9]+$'
  `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const company_id = params[2];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
        WHERE fdc.respondent_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = 1
  `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_segment") {
    const respondent_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company = ?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.respondent_id = ?  
          AND fdc.wave_id = 1
          AND fdc.option_numeric REGEXP '^[0-9]+$'
  `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      segment_id,
      company_id,
      respondent_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave_segment") {
    const respondent_id = params[0];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company = ?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.respondent_id = ?  
          AND fdc.wave_id = 1
          AND fdc.option_numeric REGEXP '^[0-9]+$'
  `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      segment_id,
      company_id,
      respondent_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 2-B) Rag Score */
export async function getRagScore(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
        WHERE fdc.respondent_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
  `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
        WHERE fdc.respondent_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
  `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_segment") {
    const respondent_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.respondent_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
  `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      segment_id,
      company_id,
      respondent_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave_segment") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.respondent_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
  `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      segment_id,
      company_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 4) Get date range according to form n filters */
export async function getDateRange(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const sql = `SELECT
                      fdc.respondent_id,
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
                      WHERE fdc.respondent_id = ?
                        AND fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_segment") {
    const respondent_id = params[0];
    const company_id = params[2];
    const sql = `SELECT
                      fdc.respondent_id,
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
                      WHERE fdc.respondent_id = ?  
                        AND fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
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
  } else if (suffix === "respondent_wave_segment") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[3];
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
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

export async function getWavePercentages(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                    									INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                                      WHERE fdc.respondent_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const [rows] = await pool.query(baseSql, [stakeholder_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                                      WHERE fdc.respondent_id = ?
                                        AND fdc.wave_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_segment") {
    const respondent_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                                      INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                          AND csu.company = ?               -- e.g. 2
                                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                                      
                                      WHERE fdc.respondent_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      segment_id,
      company_id,
      respondent_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave_segment") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                                      INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                          AND csu.company = ?               -- e.g. 2
                                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                                      
                                      WHERE fdc.respondent_id = ?
                                        AND fdc.wave_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const [rows] = await pool.query(baseSql, [
      stakeholder_id,
      segment_id,
      company_id,
      respondent_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}
///////////////////////////////////////////////////////////////////////-ENDS

//1111111111111111111111111111
//get date range for all data
export async function part1GetDateRange(respondent_id, company_id) {
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
                            ON usf.id = fdc.form_id
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
                        FROM waves w`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [respondent_id]);
  //console.log(rows);
  return rows;
}

//333333333333333333333333333333
/** 3)part3getRagDelta */
export async function part3getRagDelta(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH all_waves AS (
                        SELECT DISTINCT fdc.wave_id
                        FROM form_data_company${company_id} fdc
                        WHERE fdc.respondent_id = ?   -- e.g. 7
                    ),
                    base AS (
                        SELECT
                          fdc.respondent_id,
                          fdc.wave_id,
                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                        FROM form_data_company${company_id} AS fdc
                        WHERE fdc.respondent_id = ?
                          AND fdc.option_numeric REGEXP '^[0-9]+$'
                        GROUP BY fdc.respondent_id, fdc.wave_id
                    ),
                    merged AS (
                        SELECT 
                            aw.wave_id,
                            b.respondent_id,
                            COALESCE(b.avg_score, 0) AS avg_score
                        FROM all_waves aw
                        LEFT JOIN base b ON aw.wave_id = b.wave_id
                    ),
                    scored AS (
                        SELECT
                            respondent_id,
                            wave_id,
                            avg_score,
                            ROUND(
                                avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id),
                                2
                            ) AS delta_vs_prev_avg
                        FROM merged
                    )
                    SELECT wave_id, avg_score, COALESCE(delta_vs_prev_avg, 0) AS delta_vs_prev_avg
                    FROM scored
                    ORDER BY wave_id DESC;
                    `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [respondent_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_wave") {
    const respondent_id = params[0];
    const company_id = params[2];
    const baseSql = `WITH all_waves AS (
                        SELECT DISTINCT fdc.wave_id
                        FROM form_data_company${company_id} fdc
                        WHERE fdc.respondent_id = ?   -- e.g. 7
                    ),
                    base AS (
                        SELECT
                          fdc.respondent_id,
                          fdc.wave_id,
                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                        FROM form_data_company${company_id} AS fdc
                        WHERE fdc.respondent_id = ?
                          AND fdc.option_numeric REGEXP '^[0-9]+$'
                        GROUP BY fdc.respondent_id, fdc.wave_id
                    ),
                    merged AS (
                        SELECT 
                            aw.wave_id,
                            b.respondent_id,
                            COALESCE(b.avg_score, 0) AS avg_score
                        FROM all_waves aw
                        LEFT JOIN base b ON aw.wave_id = b.wave_id
                    ),
                    scored AS (
                        SELECT
                            respondent_id,
                            wave_id,
                            avg_score,
                            ROUND(
                                avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id),
                                2
                            ) AS delta_vs_prev_avg
                        FROM merged
                    )
                    SELECT wave_id, avg_score, COALESCE(delta_vs_prev_avg, 0) AS delta_vs_prev_avg
                    FROM scored
                    ORDER BY wave_id DESC;
                    `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [respondent_id, respondent_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_segment") {
    const respondent_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH all_waves AS (
                        SELECT DISTINCT fdc.wave_id
                        FROM form_data_company${company_id} fdc
                        WHERE fdc.respondent_id = ?   -- e.g. 7
                    ),
                    base AS (
                        SELECT
                          fdc.respondent_id,
                          fdc.wave_id,
                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                        FROM form_data_company${company_id} AS fdc
                        INNER JOIN client_segment_users AS csu
                            ON csu.segment_id = ?            -- e.g. 23
                          AND csu.company = ?               -- e.g. 2
                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                        WHERE fdc.respondent_id = ?
                          AND fdc.option_numeric REGEXP '^[0-9]+$'
                        GROUP BY fdc.respondent_id, fdc.wave_id
                    ),
                    merged AS (
                        SELECT 
                            aw.wave_id,
                            b.respondent_id,
                            COALESCE(b.avg_score, 0) AS avg_score
                        FROM all_waves aw
                        LEFT JOIN base b ON aw.wave_id = b.wave_id
                    ),
                    scored AS (
                        SELECT
                            respondent_id,
                            wave_id,
                            avg_score,
                            ROUND(
                                avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id),
                                2
                            ) AS delta_vs_prev_avg
                        FROM merged
                    )
                    SELECT wave_id, avg_score, COALESCE(delta_vs_prev_avg, 0) AS delta_vs_prev_avg
                    FROM scored
                    ORDER BY wave_id DESC
                    `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      respondent_id,
      segment_id,
      company_id,
      respondent_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave_segment") {
    const respondent_id = params[0];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH all_waves AS (
                        SELECT DISTINCT fdc.wave_id
                        FROM form_data_company${company_id} fdc
                        WHERE fdc.respondent_id = ?   -- e.g. 7
                    ),
                    base AS (
                        SELECT
                          fdc.respondent_id,
                          fdc.wave_id,
                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                        FROM form_data_company${company_id} AS fdc
                        INNER JOIN client_segment_users AS csu
                            ON csu.segment_id = ?            -- e.g. 23
                          AND csu.company = ?               -- e.g. 2
                          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                        WHERE fdc.respondent_id = ?
                          AND fdc.option_numeric REGEXP '^[0-9]+$'
                        GROUP BY fdc.respondent_id, fdc.wave_id
                    ),
                    merged AS (
                        SELECT 
                            aw.wave_id,
                            b.respondent_id,
                            COALESCE(b.avg_score, 0) AS avg_score
                        FROM all_waves aw
                        LEFT JOIN base b ON aw.wave_id = b.wave_id
                    ),
                    scored AS (
                        SELECT
                            respondent_id,
                            wave_id,
                            avg_score,
                            ROUND(
                                avg_score - LAG(avg_score) OVER (PARTITION BY respondent_id ORDER BY wave_id),
                                2
                            ) AS delta_vs_prev_avg
                        FROM merged
                    )
                    SELECT wave_id, avg_score, COALESCE(delta_vs_prev_avg, 0) AS delta_vs_prev_avg
                    FROM scored
                    ORDER BY wave_id DESC
                    `;
    //console.log(baseSql);
    const [rows] = await pool.query(baseSql, [
      respondent_id,
      segment_id,
      company_id,
      respondent_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

//44444444444444444444444444444
export async function part4getWavePercentages(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH all_waves AS (
                                      SELECT DISTINCT wave_id
                                      FROM form_data_company${company_id}
                                      WHERE respondent_id = ?
                                  ),
                                  AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      WHERE fdc.respondent_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const [rows] = await pool.query(baseSql, [
      respondent_id,
      respondent_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_wave") {
    const respondent_id = params[0];
    const company_id = params[2];
    const baseSql = `WITH all_waves AS (
                                      SELECT DISTINCT wave_id
                                      FROM form_data_company${company_id}
                                      WHERE respondent_id = ?
                                  ),
                                  AvgScores AS (
                                      SELECT
                                          fdc.wave_id,
                                          fdc.respondent_id,
                                          ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                                      FROM form_data_company${company_id} AS fdc
                                      WHERE fdc.respondent_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const [rows] = await pool.query(baseSql, [
      respondent_id,
      respondent_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix === "respondent_segment") {
    const respondent_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH all_waves AS (
                                      SELECT DISTINCT wave_id
                                      FROM form_data_company${company_id}
                                      WHERE respondent_id = ?
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
                                      WHERE fdc.respondent_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const [rows] = await pool.query(baseSql, [
      respondent_id,
      segment_id,
      company_id,
      respondent_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "respondent_wave_segment") {
    const respondent_id = params[0];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH all_waves AS (
                                      SELECT DISTINCT wave_id
                                      FROM form_data_company${company_id}
                                      WHERE respondent_id = ?
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
                                      WHERE fdc.respondent_id = ?
                                        AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const [rows] = await pool.query(baseSql, [
      respondent_id,
      segment_id,
      company_id,
      respondent_id,
      company_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

//55555555555555555555555555555
/** 3) getResponseRateNResponsesCount */
export async function part5getResponseRateNResponsesCount(filters) {
  const { suffix, params } = pickVariant(filters);
  if (suffix === "respondent") {
    const respondent_id = params[0];
    const company_id = params[1];
    return Promise.all([
      getSentCount(respondent_id, filters),
      getResponsesCount(respondent_id, company_id, filters),
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
    async function getResponsesCount(respondent_id, company_id, filters) {
      const baseSql = `WITH all_waves AS (
                                        SELECT DISTINCT fdc.wave_id
                                        FROM form_data_company${company_id} fdc
                                        WHERE fdc.respondent_id = ?
                                    ),
                                    form_wave_count AS (
                                        SELECT
                                            fdc.form_id,
                                            fdc.wave_id,
                                            COUNT(DISTINCT fdc.respondent_id) AS total_users_sum1
                                        FROM form_data_company${company_id} AS fdc
                                        WHERE fdc.respondent_id=?
                                        GROUP BY fdc.form_id, fdc.wave_id
                                        ),
                                    wave_counts AS (  
                                        SELECT 
                                          a.wave_id,
                                          SUM(a.total_users_sum1) as total_users_sum
                                        FROM form_wave_count a
                                        GROUP BY a.wave_id
                                        )
                                    SELECT
                                        aw.wave_id,
                                        COALESCE(wc.total_users_sum, 0) AS total_users_sum
                                    FROM all_waves aw
                                    LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave_id
                                    ORDER BY aw.wave_id DESC;
                                    `;
      //console.log(baseSql);
      const [responsesRows] = await pool.query(baseSql, [
        respondent_id,
        respondent_id,
      ]);
      //console.log(responsesRows);
      return responsesRows;
    }
    async function getSentCount(respondent_id, filters) {
      const baseSql = `WITH all_waves AS (
                            SELECT DISTINCT usfl.wave
                            FROM user_stakeholder_form_logs usfl
                            WHERE FIND_IN_SET(?, usfl.sent_ids) > 0
                        ),
                        wave_counts AS (
                            SELECT 
                                split_ids.wave AS wave_id,
                                COUNT(*) AS count_occurrences
                            FROM (
                                SELECT
                                  usfl.wave,
                                  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                                FROM user_stakeholder_form_logs usfl
                                JOIN numbers
                                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                                WHERE FIND_IN_SET(?, usfl.sent_ids) > 0
                            ) split_ids
                            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
                            WHERE split_ids.sent_id = ?
                            GROUP BY split_ids.wave
                        )
                        SELECT 
                            aw.wave AS wave_id,
                            COALESCE(wc.count_occurrences, 0) AS count_occurrences
                        FROM all_waves aw
                        LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave
                        ORDER BY aw.wave;
                        `;
      //console.log(baseSql);
      const [sentRows] = await pool.query(baseSql, [
        respondent_id,
        respondent_id,
        respondent_id,
      ]);
      return sentRows;
    }
  } else if (suffix == "respondent_wave") {
    const respondent_id = params[0];
    const wave_id = params[1];
    const company_id = params[2];
    return Promise.all([
      getSentCount(respondent_id, filters),
      getResponsesCount(respondent_id, company_id, filters),
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
    async function getResponsesCount(respondent_id, company_id, filters) {
      const baseSql = `WITH all_waves AS (
                                        SELECT DISTINCT fdc.wave_id
                                        FROM form_data_company${company_id} fdc
                                        WHERE fdc.respondent_id = ?
                                    ),
                                    form_wave_count AS (
                                        SELECT
                                            fdc.form_id,
                                            fdc.wave_id,
                                            COUNT(DISTINCT fdc.respondent_id) AS total_users_sum1
                                        FROM form_data_company${company_id} AS fdc
                                        WHERE fdc.respondent_id=?
                                        GROUP BY fdc.form_id, fdc.wave_id
                                        ),
                                    wave_counts AS (  
                                        SELECT 
                                          a.wave_id,
                                          SUM(a.total_users_sum1) as total_users_sum
                                        FROM form_wave_count a
                                        GROUP BY a.wave_id
                                        )
                                    SELECT
                                        aw.wave_id,
                                        COALESCE(wc.total_users_sum, 0) AS total_users_sum
                                    FROM all_waves aw
                                    LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave_id
                                    ORDER BY aw.wave_id DESC;
                                    `;
      //console.log(baseSql);
      const [responsesRows] = await pool.query(baseSql, [
        respondent_id,
        respondent_id,
      ]);
      //console.log(responsesRows);
      return responsesRows;
    }
    async function getSentCount(respondent_id, filters) {
      const baseSql = `WITH all_waves AS (
                            SELECT DISTINCT usfl.wave
                            FROM user_stakeholder_form_logs usfl
                            WHERE FIND_IN_SET(?, usfl.sent_ids) > 0
                        ),
                        wave_counts AS (
                            SELECT 
                                split_ids.wave AS wave_id,
                                COUNT(*) AS count_occurrences
                            FROM (
                                SELECT
                                  usfl.wave,
                                  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                                FROM user_stakeholder_form_logs usfl
                                JOIN numbers
                                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                                WHERE FIND_IN_SET(?, usfl.sent_ids) > 0
                            ) split_ids
                            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
                            WHERE split_ids.sent_id = ?
                            GROUP BY split_ids.wave
                        )
                        SELECT 
                            aw.wave AS wave_id,
                            COALESCE(wc.count_occurrences, 0) AS count_occurrences
                        FROM all_waves aw
                        LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave
                        ORDER BY aw.wave;
                        `;
      //console.log(baseSql);
      const [sentRows] = await pool.query(baseSql, [
        respondent_id,
        respondent_id,
        respondent_id,
      ]);
      return sentRows;
    }
  } else if (suffix === "respondent_segment") {
    const respondent_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    return Promise.all([
      getSentCount(respondent_id, segment_id, company_id, filters),
      getResponsesCount(respondent_id, segment_id, company_id, filters),
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
      respondent_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH all_waves AS (
                                        SELECT DISTINCT fdc.wave_id
                                        FROM form_data_company${company_id} fdc
                                        WHERE fdc.respondent_id = ?
                                    ),
                                    form_wave_count AS (
                                        SELECT
                                            fdc.form_id,
                                            fdc.wave_id,
                                            COUNT(DISTINCT fdc.respondent_id) AS total_users_sum1
                                        FROM form_data_company${company_id} AS fdc
                                        INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                            AND csu.company = ?               -- e.g. 2
                                            AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                                          
                                        WHERE fdc.respondent_id=?
                                        GROUP BY fdc.form_id, fdc.wave_id
                                        ),
                                    wave_counts AS (  
                                        SELECT 
                                          a.wave_id,
                                          SUM(a.total_users_sum1) as total_users_sum
                                        FROM form_wave_count a
                                        GROUP BY a.wave_id
                                        )
                                    SELECT
                                        aw.wave_id,
                                        COALESCE(wc.total_users_sum, 0) AS total_users_sum
                                    FROM all_waves aw
                                    LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave_id
                                    ORDER BY aw.wave_id DESC;
                                    `;
      //console.log(baseSql);
      const [responsesRows] = await pool.query(baseSql, [
        respondent_id,
        segment_id,
        company_id,
        respondent_id,
      ]);
      return responsesRows;
    }
    async function getSentCount(
      respondent_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH all_waves AS (
                            SELECT DISTINCT usfl.wave
                            FROM user_stakeholder_form_logs usfl
                            WHERE FIND_IN_SET(?, usfl.sent_ids) > 0
                        ),
                        wave_counts AS (
                            SELECT 
                                split_ids.wave AS wave_id,
                                COUNT(*) AS count_occurrences
                            FROM (
                                SELECT
                                  usfl.wave,
                                  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                                FROM user_stakeholder_form_logs usfl
                                JOIN numbers
                                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                                WHERE FIND_IN_SET(?, usfl.sent_ids) > 0
                            ) split_ids
                            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
                            INNER JOIN client_segment_users csu
                                ON csu.segment_id = ?         -- e.g. 23
                              AND csu.company = ?            -- e.g. 2
                              AND FIND_IN_SET(split_ids.sent_id, csu.client_users) > 0                                
                            WHERE split_ids.sent_id = ?
                            GROUP BY split_ids.wave
                        )
                        SELECT 
                            aw.wave AS wave_id,
                            COALESCE(wc.count_occurrences, 0) AS count_occurrences
                        FROM all_waves aw
                        LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave
                        ORDER BY aw.wave;
                        `;
      const [sentRows] = await pool.query(baseSql, [
        respondent_id,
        respondent_id,
        segment_id,
        company_id,
        respondent_id,
      ]);
      return sentRows;
    }
  } else if (suffix == "respondent_wave_segment") {
    const respondent_id = params[0];
    const segment_id = params[2];
    const company_id = params[3];
    return Promise.all([
      getSentCount(respondent_id, segment_id, company_id, filters),
      getResponsesCount(respondent_id, segment_id, company_id, filters),
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
      respondent_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH all_waves AS (
                                        SELECT DISTINCT fdc.wave_id
                                        FROM form_data_company${company_id} fdc
                                        WHERE fdc.respondent_id = ?
                                    ),
                                    form_wave_count AS (
                                        SELECT
                                            fdc.form_id,
                                            fdc.wave_id,
                                            COUNT(DISTINCT fdc.respondent_id) AS total_users_sum1
                                        FROM form_data_company${company_id} AS fdc
                                        INNER JOIN client_segment_users AS csu
                                            ON csu.segment_id = ?            -- e.g. 23
                                            AND csu.company = ?               -- e.g. 2
                                            AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                                          
                                        WHERE fdc.respondent_id=?
                                        GROUP BY fdc.form_id, fdc.wave_id
                                        ),
                                    wave_counts AS (  
                                        SELECT 
                                          a.wave_id,
                                          SUM(a.total_users_sum1) as total_users_sum
                                        FROM form_wave_count a
                                        GROUP BY a.wave_id
                                        )
                                    SELECT
                                        aw.wave_id,
                                        COALESCE(wc.total_users_sum, 0) AS total_users_sum
                                    FROM all_waves aw
                                    LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave_id
                                    ORDER BY aw.wave_id DESC;
                                    `;
      //console.log(baseSql);
      const [responsesRows] = await pool.query(baseSql, [
        respondent_id,
        segment_id,
        company_id,
        respondent_id,
      ]);
      return responsesRows;
    }
    async function getSentCount(
      respondent_id,
      segment_id,
      company_id,
      filters
    ) {
      const baseSql = `WITH all_waves AS (
                            SELECT DISTINCT usfl.wave
                            FROM user_stakeholder_form_logs usfl
                            WHERE FIND_IN_SET(?, usfl.sent_ids) > 0
                        ),
                        wave_counts AS (
                            SELECT 
                                split_ids.wave AS wave_id,
                                COUNT(*) AS count_occurrences
                            FROM (
                                SELECT
                                  usfl.wave,
                                  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(usfl.sent_ids, ',', numbers.n), ',', -1)) AS sent_id
                                FROM user_stakeholder_form_logs usfl
                                JOIN numbers
                                  ON numbers.n <= 1 + LENGTH(usfl.sent_ids) - LENGTH(REPLACE(usfl.sent_ids, ',', ''))
                                WHERE FIND_IN_SET(?, usfl.sent_ids) > 0
                            ) split_ids
                            INNER JOIN client_users AS cu ON cu.id = split_ids.sent_id
                            INNER JOIN client_segment_users csu
                                ON csu.segment_id = ?         -- e.g. 23
                              AND csu.company = ?            -- e.g. 2
                              AND FIND_IN_SET(split_ids.sent_id, csu.client_users) > 0                                
                            WHERE split_ids.sent_id = ?
                            GROUP BY split_ids.wave
                        )
                        SELECT 
                            aw.wave AS wave_id,
                            COALESCE(wc.count_occurrences, 0) AS count_occurrences
                        FROM all_waves aw
                        LEFT JOIN wave_counts wc ON wc.wave_id = aw.wave
                        ORDER BY aw.wave;
                        `;
      const [sentRows] = await pool.query(baseSql, [
        respondent_id,
        respondent_id,
        segment_id,
        company_id,
        respondent_id,
      ]);
      return sentRows;
    }
  }
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/** 1) Respondent Meta */
export async function getRespondentMeta(clientUserId, stakeholder_id) {
  //console.log("clientUserId", clientUserId);
  /*   const sql = `
            SELECT u.id, u.name, u.email, u.image, u.role, u.organization, u.stakeholder_id, stk.name as stakeholder_name, stk.image as stakeholder_image, stk.image_name as stakeholder_image_name, u.company
            FROM client_users u
            LEFT JOIN \`stakeholder\` stk ON u.stakeholder_id = stk.id
            WHERE u.id = ?
            `; */
  const sql = `
            SELECT u.id, u.name, u.company
            FROM client_users u
            WHERE u.id = ?
            `;
  //console.log(sql);
  const [rows] = await pool.query(sql, [clientUserId, stakeholder_id]);
  return rows[0] || {};
}

/** 2) getAllSegments */
export async function getAllSegments(clientUserId, stakeholder_id) {
  /*   const sql = `SELECT 
                      s.name as label,
                      s.id as value
                  FROM user_stakeholder_form usf
                  LEFT JOIN segment s 
                      ON FIND_IN_SET(s.id, usf.segments) > 0
                  WHERE usf.id = ?
                  ORDER BY s.name ASC`; */
  const sql = `SELECT s.name as label, s.id as value
                FROM client_segment_users csu
                LEFT JOIN segment AS s ON s.id=csu.segment_id AND s.stakeholder_id = ?
                WHERE FIND_IN_SET(?, csu.client_users) > 0
                ORDER BY s.name ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholder_id, clientUserId]);
  return rows;
}

/** 3) getAllWaves */
export async function getAllWaves(clientUserId, stakeholder_id) {
  /*   const sql = `SELECT DISTINCT CONCAT("Wave ",wave_id)as label, wave_id as value FROM \`wave_question_avg\` where form_id = ? ORDER BY wave_id ASC`; */
  const sql = `SELECT DISTINCT CONCAT("Wave ",usfl.wave)as label, usfl.wave as value
              FROM user_stakeholder_form_logs usfl
              INNER JOIN user_stakeholder_form usf ON usfl.user_stakeholder_form_id = usf.id AND usf.stakeholder_id = ?
              WHERE FIND_IN_SET(?, usfl.sent_ids) > 0
              ORDER BY usfl.wave ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholder_id, clientUserId]);
  return rows;
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
