// services/ragTrend.service.js
import pool from "../db";

//////////////////////////FORM/////////////////////////////////////////////-STARTS
export async function getFormMeta(formId) {
  const sql = `
    SELECT
      usf.company,
      usf.frequency,
      stk.name AS stakeholder_name
    FROM user_stakeholder_form usf
    LEFT JOIN stakeholder stk ON usf.stakeholder_id = stk.id
    WHERE usf.id = ?
    LIMIT 1
  `;
  const [rows] = await pool.query(sql, [formId]);
  return rows?.[0] || null;
}

export async function getWaveAvgRagNDelta(formId) {
  const sql = `
    SELECT
      wqa.wave_id,
      ROUND(AVG(wqa.average), 2)             AS avg_rag,
      ROUND(AVG(wqa.delta_vs_prev_avg), 2)   AS delta_vs_prev_avg,
      DATE_FORMAT(usfl.created_at, '%d %M %Y') AS wave_date
    FROM wave_question_avg wqa
    LEFT JOIN user_stakeholder_form_logs usfl
      ON wqa.form_id = usfl.user_stakeholder_form_id
     AND usfl.wave    = wqa.wave_id
    WHERE wqa.form_id = ?
      AND wqa.question_type != "single_select"
    GROUP BY wqa.wave_id
    ORDER BY wqa.wave_id DESC
  `;
  const [rows] = await pool.query(sql, [formId]);
  return rows;
}

export async function getWaveCategoryPercentages(formId, company) {
  // Validate the dynamic suffix is a simple integer
  const companySuffix = Number(company);
  if (!Number.isFinite(companySuffix)) {
    throw new Error("Invalid company id for dynamic table");
  }

  const sql = `
    WITH AvgScores AS (
      SELECT
        wave_id,
        respondent_id,
        ROUND(AVG(option_numeric), 2) AS avg_score
      FROM \`form_data_company${companySuffix}\`
      WHERE form_id = ?
        AND option_numeric REGEXP '^[0-9]+$'
      GROUP BY wave_id, respondent_id
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
    GROUP BY wave_id
    ORDER BY wave_id DESC
  `;
  const [rows] = await pool.query(sql, [formId]);
  return rows;
}

export function mergeWaveRows({ waveAvgRows, percentRows }) {
  const byWave = new Map(percentRows.map((r) => [Number(r.wave_id), r]));
  return (waveAvgRows || []).map((w) => {
    const waveId = Number(w.wave_id);
    const p = byWave.get(waveId);
    const merged = p ? { ...w, ...p } : { ...w };

    // Keep only the desired fields
    const {
      wave_id,
      avg_rag,
      delta_vs_prev_avg,
      wave_date,
      amber_percentage,
      red_percentage,
      green_percentage,
    } = merged;

    return {
      wave_id,
      avg_rag,
      delta_vs_prev_avg,
      wave_date,
      amber_percentage,
      red_percentage,
      green_percentage,
    };
  });
}
//////////////////////////FORM/////////////////////////////////////////////-ENDS
//////////////////////////QQQQQQQQQQQQQQQQQQQQ//////////////////////////////////////-STARTS
export async function getWaveAvgRagNDeltaQ(formId, questionId) {
  const sql = `
  SELECT
      wqa.wave_id,
      ROUND(wqa.average, 2)             AS avg_rag,
      ROUND(wqa.delta_vs_prev_avg, 2)   AS delta_vs_prev_avg,
      DATE_FORMAT(usfl.created_at, '%d %M %Y') AS wave_date
    FROM wave_question_avg wqa
    LEFT JOIN user_stakeholder_form_logs usfl
      ON wqa.form_id = usfl.user_stakeholder_form_id
     AND usfl.wave    = wqa.wave_id
    WHERE wqa.form_id = ? AND wqa.question_id = ?
      AND wqa.question_type != "single_select"  
    ORDER BY wqa.wave_id DESC
  `;
  const [rows] = await pool.query(sql, [formId, questionId]);
  //console.log("Wave Avg Rows:", rows);
  return rows;
}

export async function getWaveCategoryPercentagesQ(formId, company, questionId) {
  // Validate the dynamic suffix is a simple integer
  const companySuffix = Number(company);
  if (!Number.isFinite(companySuffix)) {
    throw new Error("Invalid company id for dynamic table");
  }

  const sql = `
    WITH AvgScores AS (
      SELECT
        wave_id,
        respondent_id,
        ROUND(AVG(option_numeric), 2) AS avg_score
      FROM \`form_data_company${companySuffix}\`
      WHERE form_id = ? AND question_id = ?
        AND option_numeric REGEXP '^[0-9]+$'
      GROUP BY wave_id, respondent_id
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
    GROUP BY wave_id
    ORDER BY wave_id DESC
  `;
  const [rows] = await pool.query(sql, [formId, questionId]);
  //console.log("Wave Category Percentages:", rows);
  return rows;
}
//////////////////////////QQQQQQQQQQQQQQQQQQQQ//////////////////////////////////////-ENDS
