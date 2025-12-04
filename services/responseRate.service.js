// services/responseRate.service.js
import pool from "../db";

/** Q1: company + stakeholder info */
export async function getCompanyInfo(stakeholderFormId) {
  const sql = `
    SELECT usf.company, usf.frequency, stk.name AS stakeholder_name
    FROM user_stakeholder_form usf
    LEFT JOIN stakeholder stk ON usf.stakeholder_id = stk.id
    WHERE usf.id = ?
  `;
  return await pool.query(sql, [stakeholderFormId]).then(([rows]) => rows);
}

/** Q2: responses per wave (unique wave rows) */
export async function getResponsesRows(stakeholderFormId) {
  const sql = `
    SELECT wqa.form_id, wqa.wave_id AS wave, wqa.company_id, wqa.total_users AS total_response_users, DATE_FORMAT(usfl.created_at, '%d %M %Y') AS wave_date
    FROM wave_question_avg wqa
    LEFT JOIN user_stakeholder_form_logs usfl
      ON wqa.form_id = usfl.user_stakeholder_form_id
      AND usfl.wave = wqa.wave_id
    WHERE wqa.form_id = ?
    GROUP BY wqa.wave_id
    ORDER BY wqa.wave_id DESC
  `;
  return await pool.query(sql, [stakeholderFormId]).then(([rows]) => rows);
}

/** Q3: emails sent per wave */
export async function getFormsSentRows(stakeholderFormId) {
  const sql = `
    SELECT wave, tot_email_sent
    FROM user_stakeholder_form_logs
    WHERE user_stakeholder_form_id = ?
  `;
  return await pool.query(sql, [stakeholderFormId]).then(([rows]) => rows);
}
