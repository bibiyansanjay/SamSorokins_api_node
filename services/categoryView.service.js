// services/formView.service.js
import pool from "../db";

/**
 * Filters supported: subcategory_id, wave_id, segment_id
 * Each is optional and accepts at most ONE value.
 * We pick a SQL variant by the filter combo to avoid string concat.
 */
//((((((((((((((((((((((((((((((((((((((helpers starts))))))))))))))))))))))))))))))))))))))
function pickVariant({ subcategory_id, company_id, wave_id, segment_id }) {
  // if (!subcategory_id && !wave_id && !segment_id)
  //   return { suffix: "none", params: [] };
  if (subcategory_id && company_id && !wave_id && !segment_id)
    return { suffix: "subcategory", params: [subcategory_id, company_id] };
  if (subcategory_id && company_id && wave_id && !segment_id)
    return {
      suffix: "subcategory_wave",
      params: [subcategory_id, company_id, wave_id],
    };
  if (subcategory_id && company_id && !wave_id && segment_id)
    return {
      suffix: "subcategory_segment",
      params: [subcategory_id, company_id, segment_id],
    };
  if (subcategory_id && company_id && wave_id && segment_id)
    return {
      suffix: "subcategory_wave_segment",
      params: [subcategory_id, company_id, wave_id, segment_id],
    };
  throw new Error("Unsupported filter combination");
}
/** Parse "1, 2,5" -> [1,2,5] */
function parseCsvIds(csv) {
  if (!csv) return { list: [], set: new Set() };
  const list = String(csv)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map(Number)
    .filter(Number.isFinite);
  return { list, set: new Set(list) };
}
/**
 * Get per-wave counts of sent IDs that belong to a segment's client_users.
 * Returns an array of rows, one per wave.
 */

export async function getWaveSegmentDelivery({ company, segment_id, form_id }) {
  // A) segment users
  const [segRows] = await pool.query(
    `SELECT client_users FROM client_segment_users WHERE company = ? AND segment_id = ?`,
    [company, segment_id]
  );
  //console.log("segRows", segRows);
  const clientUsersCsv = segRows?.[0]?.client_users || "";
  //console.log("clientUsersCsv", clientUsersCsv);
  const { list: segmentUsersList, set: segmentUsersSet } =
    parseCsvIds(clientUsersCsv);

  // B) waves + sent ids
  const [logRows] = await pool.query(
    `SELECT user_stakeholder_form_id, wave, sent_ids
     FROM user_stakeholder_form_logs
     WHERE user_stakeholder_form_id = ?
     ORDER BY wave`,
    [form_id]
  );
  //console.log("logRows", logRows);
  // Build per-wave breakdown
  return logRows.map((r) => {
    const { list: sentList, set: sentSet } = parseCsvIds(r.sent_ids);
    const matched = sentList.filter((id) => segmentUsersSet.has(id));
    const uniqMatched = [...new Set(matched)];
    /*     const kk = {
      user_stakeholder_form_id: Number(r.user_stakeholder_form_id),
      company: Number(company),
      segment_id: Number(segment_id),
      wave: Number(r.wave),
      sent_in_segment: uniqMatched.length,
      total_sent: sentSet.size,
      segment_user_count: segmentUsersList.length,
      matched_user_ids: uniqMatched.join(","),
    }; */
    //console.log("kk", kk);
    return {
      user_stakeholder_form_id: Number(r.user_stakeholder_form_id),
      company: Number(company),
      segment_id: Number(segment_id),
      wave: Number(r.wave),
      sent_in_segment: uniqMatched.length,
      total_sent: sentSet.size,
      segment_user_count: segmentUsersList.length,
      matched_user_ids: uniqMatched.join(","),
    };
  });
}
//((((((((((((((((((((((((((((((((((((((helpers ends))))))))))))))))))))))))))))))))))))))

/** 1) getAllSegments */
export async function getAllSegments(subcategoryId, company) {
  const sql = `SELECT 
                      s.name as label,
                      s.id as value
                  FROM user_subcategory_form usubf
                  LEFT JOIN user_stakeholder_form usf
                      ON usubf.id = usf.user_subcategory_form_id
                  LEFT JOIN segment s 
                      ON FIND_IN_SET(s.id, usf.segments) > 0
                  WHERE usubf.subcategory_id = ? AND usubf.company = ?
                  GROUP BY s.id
                  ORDER BY s.name ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [subcategoryId, company]);
  //console.log(rows);
  return rows;
}

/** 2) getAllWaves */
export async function getAllWaves(subcategoryId, company) {
  const sql = `SELECT DISTINCT CONCAT("Wave ",wave_id)as label, wave_id as value FROM \`wave_question_avg\` where subcategory_id = ? AND company_id = ? ORDER BY wave_id ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [subcategoryId, company]);
  //console.log(rows);
  return rows;
}

/** 3) Subcategory data */
export async function getSubcategoryData(subcategoryId) {
  const sql = `
    SELECT sub.id as subcategory_id, sub.name as subcategory_name, sub.image as subcategory_image, sub.image_name as subcategory_image_name
    FROM subcategory sub  
    WHERE sub.id = ? 
    LIMIT 1`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [subcategoryId]);
  return rows;
}

/** 4) Rag Score */
const SQL_RAG = {
  subcategory: `
    SELECT 
    ROUND(AVG(average), 2) AS rag_score 
    FROM wave_question_avg
    WHERE subcategory_id=? AND company_id = ? AND question_type != "single_select"`,
  subcategory_wave: `
    SELECT 
    ROUND(AVG(average), 2) AS rag_score 
    FROM wave_question_avg
    WHERE subcategory_id=? AND company_id = ? AND wave_id=? AND question_type != "single_select"`,
};

export async function getRagScore(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "subcategory_segment") {
    const subcategory_id = params[0];
    const company = params[1];
    const segment_id = params[2];
    const sql = `
    SELECT 
          -- COUNT(fdc.option_numeric) AS count,
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company} AS fdc
        LEFT JOIN user_stakeholder_form AS usf ON fdc.form_id = usf.id
        LEFT JOIN user_subcategory_form AS usubf ON usf.user_subcategory_form_id  = usubf.id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE usubf.subcategory_id = ? AND usubf.company = ?
          AND fdc.option_numeric REGEXP '^[0-9]+$'
  `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company,
      subcategory_id,
      company,
    ]);
    console.log(rows);
    return rows;
  } else if (suffix == "subcategory_wave_segment") {
    const subcategory_id = params[0];
    const company = params[1];
    const wave_id = params[2];
    const segment_id = params[3];
    const sql = `
    SELECT 
          -- COUNT(fdc.option_numeric) AS count,
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company} AS fdc
        LEFT JOIN user_stakeholder_form AS usf ON fdc.form_id = usf.id
        LEFT JOIN user_subcategory_form AS usubf ON usf.user_subcategory_form_id  = usubf.id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE usubf.subcategory_id = ? AND usubf.company = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
  `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company,
      subcategory_id,
      company,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  } else {
    const sql = SQL_RAG[suffix];
    const [rows] = await pool.query(sql, params);
    //console.log(rows);
    return rows;
  }
}

/** 5) Response Percent (requires subcategory_id) */
export async function getResponsePercent(filters) {
  const { suffix, params } = pickVariant(filters);
  const subcategory_id = params[0];
  const company = params[1];
  //
  if (suffix === "subcategory") {
    const [responsesRows] = await pool.query(
      `SELECT SUM(total_users) AS total_users_sum 
     FROM ( SELECT total_users 
            FROM wave_question_avg 
            WHERE subcategory_id = ? AND company_id = ? 
            GROUP BY wave_id ) AS grouped_data`,
      [subcategory_id, company]
    );

    const [formsSentRows] = await pool.query(
      `SELECT SUM(tot_email_sent) as tot_email_sent
     FROM user_stakeholder_form_logs
     WHERE user_stakeholder_form_id = ?`,
      [subcategory_id]
    );

    const totalResponses = responsesRows?.[0]?.total_users_sum || 0;
    console.log("totalResponses", totalResponses);
    const totalSent = formsSentRows?.[0]?.tot_email_sent || 0;
    console.log("totalSent", totalSent);

    const responsePercentage =
      totalSent > 0
        ? Math.round((totalResponses / totalSent) * 100) + "%"
        : "0%";

    return { totalResponses, totalSent, responsePercentage };
  } else if (suffix == "subcategory_wave") {
    const wave_id = params[2];
    //console.log(wave_id);
    const [responsesRows] = await pool.query(
      `SELECT form_id, wave_id, total_users FROM \`wave_question_avg\` where form_id=? and wave_id=? LIMIT 1`,
      [form_id, wave_id]
    );

    const [formsSentRows] = await pool.query(
      `SELECT tot_email_sent
     FROM user_stakeholder_form_logs
     WHERE user_stakeholder_form_id = ? AND wave = ?`,
      [form_id, wave_id]
    );

    const totalResponses = responsesRows?.[0]?.total_users || 0;
    const totalSent = formsSentRows?.[0]?.tot_email_sent || 0;

    const responsePercentage =
      totalSent > 0
        ? Math.round((totalResponses / totalSent) * 100) + "%"
        : "0%";

    return { totalResponses, totalSent, responsePercentage };
  } else if (suffix === "subcategory_segment") {
    const segment_id = params[2];
    //console.log(segment_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };

    const [responsesRows] = await pool.query(
      `WITH base AS (
    		SELECT
            fdc.form_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
          FROM form_data_company${company} AS fdc
            INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
          WHERE fdc.form_id = ?
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
           )
           SELECT form_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`,
      [segment_id, company, form_id]
    );
    ////////////////////////////////////////////////////////////////////
    const breakdown = await getWaveSegmentDelivery({
      company,
      segment_id,
      form_id,
    });
    // No wave_id -> sum across all waves
    const total_sent_in_segment = breakdown.reduce(
      (acc, r) => acc + r.sent_in_segment,
      0
    );
    /*     return {
      company: Number(company),
      segment_id: Number(segment_id),
      form_id: Number(form_id),
      sent_in_segment: total_sent_in_segment, // sum across waves
      mode: "all_waves_sum",
      breakdown,
    }; */
    ////////////////////////////////////////////////////////////////////
    const totalResponses = responsesRows?.[0]?.total_users_sum || 0;
    const totalSent = total_sent_in_segment || 0;

    const responsePercentage =
      totalSent > 0
        ? Math.round((totalResponses / totalSent) * 100) + "%"
        : "0%";

    return { totalResponses, totalSent, responsePercentage };
  } else if (suffix == "subcategory_wave_segment") {
    const wave_id = params[2];
    const segment_id = params[3];
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };

    const [responsesRows] = await pool.query(
      `SELECT
            fdc.form_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
          FROM form_data_company${company} AS fdc
            INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
          WHERE fdc.form_id = ? AND fdc.wave_id=?
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC`,
      [segment_id, company, form_id, wave_id]
    );
    ////////////////////////////////////////////////////////////////////
    const breakdown = await getWaveSegmentDelivery({
      company,
      segment_id,
      form_id,
    });
    // Specific wave_id
    const waveNum = Number(wave_id);
    const row = breakdown.find((r) => r.wave === waveNum);
    // return {
    //   company: Number(company),
    //   segment_id: Number(segment_id),
    //   form_id: Number(form_id),
    //   wave_id: waveNum,
    //   sent_in_segment: row ? row.sent_in_segment : 0, // 0 if that wave has no row
    //   mode: "single_wave",
    //   breakdown,
    // };
    ////////////////////////////////////////////////////////////////////
    const totalResponses = responsesRows?.[0]?.total_users_sum || 0;
    const totalSent = row ? row.sent_in_segment : 0;

    const responsePercentage =
      totalSent > 0
        ? Math.round((totalResponses / totalSent) * 100) + "%"
        : "0%";

    return { totalResponses, totalSent, responsePercentage };
  }
}

/** 3) Top 3 Scores */
const SQL_TOP3 = {
  /*   none: `
    SELECT wqa.question_id, q.three_word_outcome_en, 
    -- q.three_word_outcome_nl, 
           ROUND(AVG(wqa.average), 2) AS rag_score_avg
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    WHERE wqa.question_type != "single_select"
    GROUP BY question_id 
    ORDER BY rag_score_avg DESC LIMIT 3`, */
  form: `
    SELECT wqa.question_id, q.three_word_outcome_en, 
    -- q.three_word_outcome_nl , 
           ROUND(AVG(wqa.average), 2) AS rag_score_avg
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    WHERE wqa.form_id=? AND wqa.question_type != "single_select"
    GROUP BY question_id 
    ORDER BY rag_score_avg DESC LIMIT 3`,
  form_wave: `
    SELECT wqa.question_id, q.three_word_outcome_en, 
    -- q.three_word_outcome_nl , 
           ROUND(AVG(wqa.average), 2) AS rag_score_avg
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    WHERE wqa.form_id=? AND wqa.wave_id=? AND wqa.question_type != "single_select"
    GROUP BY question_id 
    ORDER BY rag_score_avg DESC LIMIT 3`,
};
export async function getTop3Scores(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "form_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const segment_id = params[1];
    //console.log(segment_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `SELECT 
        fdc.question_id, q.three_word_outcome_en, 
        -- q.three_word_outcome_nl,
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score_avg
        FROM form_data_company${company} AS fdc
        LEFT JOIN question q ON q.id = fdc.question_id 
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? 
          AND fdc.option_numeric REGEXP '^[0-9]+$'
        GROUP BY fdc.question_id 
        ORDER BY rag_score_avg DESC LIMIT 3`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [segment_id, company, form_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const wave_id = params[1];
    //console.log(wave_id);
    const segment_id = params[2];
    //console.log(segment_id);

    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `SELECT 
        fdc.question_id, q.three_word_outcome_en, 
        -- q.three_word_outcome_nl,
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score_avg
        FROM form_data_company${company} AS fdc
        LEFT JOIN question q ON q.id = fdc.question_id 
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? 
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
        GROUP BY fdc.question_id 
        ORDER BY rag_score_avg DESC LIMIT 3`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company,
      form_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  } else {
    const sql = SQL_TOP3[suffix];
    const [rows] = await pool.query(sql, params);
    return rows;
  }
}

/** 4) Bottom 3 Scores */
const SQL_BOTTOM3 = {
  /*   none: `
    SELECT wqa.question_id, q.three_word_outcome_en, 
    -- q.three_word_outcome_nl , 
           ROUND(AVG(wqa.average), 2) AS rag_score_avg
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    WHERE wqa.question_type != "single_select"
    GROUP BY question_id 
    ORDER BY rag_score_avg ASC LIMIT 3`, */
  form: `
    SELECT wqa.question_id, q.three_word_outcome_en, 
    -- q.three_word_outcome_nl , 
           ROUND(AVG(wqa.average), 2) AS rag_score_avg
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    WHERE wqa.form_id=? AND wqa.question_type != "single_select"
    GROUP BY question_id 
    ORDER BY rag_score_avg ASC LIMIT 3`,
  form_wave: `
    SELECT wqa.question_id, q.three_word_outcome_en, 
    -- q.three_word_outcome_nl , 
           ROUND(AVG(wqa.average), 2) AS rag_score_avg
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    WHERE wqa.form_id=? AND wqa.wave_id=? AND wqa.question_type != "single_select"
    GROUP BY question_id 
    ORDER BY rag_score_avg ASC LIMIT 3`,
};
export async function getBottom3Scores(filters) {
  const { suffix, params } = pickVariant(filters);
  if (suffix == "form_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const segment_id = params[1];
    //console.log(segment_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `SELECT 
        fdc.question_id, q.three_word_outcome_en, 
        -- q.three_word_outcome_nl,
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score_avg
        FROM form_data_company${company} AS fdc
        LEFT JOIN question q ON q.id = fdc.question_id 
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? 
          AND fdc.option_numeric REGEXP '^[0-9]+$'
        GROUP BY fdc.question_id 
        ORDER BY rag_score_avg ASC LIMIT 3`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [segment_id, company, form_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const wave_id = params[1];
    //console.log(wave_id);
    const segment_id = params[2];
    //console.log(segment_id);

    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `SELECT 
        fdc.question_id, q.three_word_outcome_en, 
        -- q.three_word_outcome_nl,
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score_avg
        FROM form_data_company${company} AS fdc
        LEFT JOIN question q ON q.id = fdc.question_id 
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? 
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
        GROUP BY fdc.question_id 
        ORDER BY rag_score_avg ASC LIMIT 3`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company,
      form_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  } else {
    const sql = SQL_BOTTOM3[suffix];
    const [rows] = await pool.query(sql, params);
    return rows;
  }
}

/** 5) Trend Movers */
const SQL_TREND_MOVERS = {
  /*   none: `
    SELECT wqa.question_id, q.three_word_outcome_en, 
    -- q.three_word_outcome_nl, 
           wqa.delta_vs_prev_avg
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    WHERE wqa.question_type != "single_select"
    ORDER BY ABS(wqa.delta_vs_prev_avg) DESC LIMIT 3`, */
  form: `
    SELECT wqa.form_id, wqa.wave_id, wqa.question_id, q.three_word_outcome_en, 
    -- q.three_word_outcome_nl, 
    wqa.average as rag_score_avg, wqa.delta_vs_prev_avg
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    WHERE wqa.form_id=? AND wqa.question_type != "single_select"
    ORDER BY ABS(wqa.delta_vs_prev_avg) DESC LIMIT 3`,
  form_wave: `
    SELECT wqa.form_id, wqa.wave_id, wqa.question_id, q.three_word_outcome_en, 
    -- q.three_word_outcome_nl, 
    wqa.average as rag_score_avg, wqa.delta_vs_prev_avg
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    WHERE wqa.form_id=? AND wqa.wave_id=? AND wqa.question_type != "single_select"
    ORDER BY ABS(wqa.delta_vs_prev_avg) DESC LIMIT 3`,
};
export async function getTrendMovers(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "form_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const segment_id = params[1];
    //console.log(segment_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      -- q.three_word_outcome_nl,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company} AS fdc
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 24
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.wave_id, fdc.question_id, q.three_word_outcome_en
                    -- , q.three_word_outcome_nl
                  )
                  SELECT
                    form_id,
                    wave_id,
                    question_id,
                    three_word_outcome_en,
                    -- three_word_outcome_nl,
                    ROUND(avg_score, 2) AS rag_score_avg,
                    ROUND(
                      avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id),
                      2
                    ) AS delta_vs_prev_avg
                  FROM base
                  ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [segment_id, company, form_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const wave_id = params[1];
    //console.log(wave_id);
    const segment_id = params[2];
    //console.log(segment_id);

    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `WITH base AS (
                        SELECT
                          fdc.form_id,
                          fdc.wave_id,
                          fdc.question_id,
                          q.three_word_outcome_en,
                          -- q.three_word_outcome_nl,
                          AVG(fdc.option_numeric) AS avg_score
                        FROM form_data_company${company} AS fdc
                        LEFT JOIN question q
                          ON q.id = fdc.question_id
                        INNER JOIN client_segment_users AS csu
                          ON csu.segment_id = ?         -- e.g. 23
                        AND csu.company = ?           -- e.g. 2
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                        WHERE fdc.form_id = ?           -- e.g. 24
                          AND fdc.option_numeric REGEXP '^[0-9]+$'
                        GROUP BY fdc.wave_id, fdc.question_id, q.three_word_outcome_en
                        -- , q.three_word_outcome_nl
                      ),
                      scored AS (
                        SELECT
                          form_id,
                          wave_id,
                          question_id,
                          three_word_outcome_en,
                          -- three_word_outcome_nl,
                          ROUND(avg_score, 2) AS rag_score_avg,
                          ROUND(
                            avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id),
                            2
                          ) AS delta_vs_prev_avg
                        FROM base
                      )
                      SELECT *
                      FROM scored
                      WHERE wave_id = ?                 -- e.g. 3
                      ORDER BY ABS(delta_vs_prev_avg) DESC LIMIT 3`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company,
      form_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  } else {
    const sql = SQL_TREND_MOVERS[suffix];
    const [rows] = await pool.query(sql, params);
    return rows;
  }
}

/** 6) Question details with delta_vs_prev_avg */
const SQL_Q_DETAILS = {
  form: `
    SELECT 
      wqa.form_id, wqa.wave_id, wqa.question_id,
      q.question_text_en, q.question_text_nl,
      ROUND(AVG(wqa.average), 2) AS rag_score_avg, q.question_type as question_type_id, qt.name as question_type_name, 
      MAX(
        CASE WHEN wqa.wave_id = (
          SELECT MAX(wqa_sub.wave_id)
          FROM wave_question_avg wqa_sub
          WHERE wqa_sub.form_id = wqa.form_id
            AND wqa_sub.question_id = wqa.question_id
          --  AND wqa_sub.question_type != 'single_select' 
        ) THEN wqa.delta_vs_prev_avg END
      ) AS delta_vs_prev_avg
    FROM wave_question_avg wqa
    LEFT JOIN question q ON q.id = wqa.question_id
    LEFT JOIN question_type qt ON qt.id = q.question_type
    WHERE wqa.form_id = ? 
    -- AND wqa.question_type != 'single_select'
    GROUP BY wqa.question_id
    ORDER BY wqa.question_id ASC`,
  form_wave: `
    SELECT wqa.form_id, wqa.wave_id, wqa.question_id, q.question_text_en, q.question_text_nl, wqa.average AS rag_score_avg, wqa.delta_vs_prev_avg, q.question_type as question_type_id, qt.name as question_type_name 
    FROM wave_question_avg wqa 
    LEFT JOIN question q ON q.id = wqa.question_id 
    LEFT JOIN question_type qt ON qt.id = q.question_type 
    WHERE wqa.form_id = ? 
    -- AND wqa.question_type != 'single_select' 
    AND wqa.wave_id=? 
    GROUP BY wqa.question_id 
    ORDER BY wqa.question_id ASC`,
};
export async function getQuestionDetailsWithDelta(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "form_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const segment_id = params[1];
    //console.log(segment_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      -- q.three_word_outcome_en,
                      -- q.three_word_outcome_nl,
                      q.question_text_en,
                      q.question_text_nl,
                      q.question_type as question_type_id,
                      qt.name as question_type_name,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company} AS fdc
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    LEFT JOIN question_type qt
                      ON qt.id = q.question_type
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 24
                    --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.wave_id, fdc.question_id
                    -- , q.three_word_outcome_en, q.three_word_outcome_nl
                  ),
                scored AS (      
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      -- three_word_outcome_en,
                      -- three_word_outcome_nl,
                      question_text_en,
                      question_text_nl,
                      question_type_id,
                      question_type_name,
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
                )
                  SELECT
                      form_id,
                      question_id,
                      question_text_en,
                      question_text_nl,
                      question_type_id,
                      question_type_name,
                      ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this question
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                      -- MAX(max_wave_for_q) AS latest_wave
                    FROM scored_with_max
                    GROUP BY form_id, question_id
                    ORDER BY question_id
                `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [segment_id, company, form_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const wave_id = params[1];
    //console.log(wave_id);
    const segment_id = params[2];
    //console.log(segment_id);

    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      fdc.question_id,
                      -- q.three_word_outcome_en,
                      -- q.three_word_outcome_nl,
                      q.question_text_en,
                      q.question_text_nl,
                      q.question_type as question_type_id,
                      qt.name as question_type_name,
                      AVG(fdc.option_numeric) AS avg_score
                    FROM form_data_company${company} AS fdc
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    LEFT JOIN question_type qt
                      ON qt.id = q.question_type
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 24
                    --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                    GROUP BY fdc.wave_id, fdc.question_id
                    -- , q.three_word_outcome_en, q.three_word_outcome_nl
                  ),
                scored AS (    
                    SELECT
                      form_id,
                      wave_id,
                      question_id,
                      -- three_word_outcome_en,
                      -- three_word_outcome_nl,
                      question_text_en,
                      question_text_nl,
                      question_type_id,
                      question_type_name,
                      ROUND(avg_score, 2) AS rag_score_avg,
                      ROUND(
                        avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id),
                        2
                      ) AS delta_vs_prev_avg
                    FROM base     
                  )
                SELECT * FROM scored WHERE wave_id = ?
                `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company,
      form_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  } else {
    const sql = SQL_Q_DETAILS[suffix];
    const [rows] = await pool.query(sql, params);
    return rows;
  }
}

/** 7) Trend (avg delta for latest/selected wave) */
const SQL_TREND = {
  /* none: `
    SELECT 
      (SELECT MAX(wave_id) FROM wave_question_avg WHERE question_type != 'single_select') AS wave_id,
      ROUND(AVG(delta_vs_prev_avg), 2) AS delta_vs_prev_avg
    FROM wave_question_avg
    WHERE question_type != 'single_select'
      AND wave_id = (SELECT MAX(wave_id) FROM wave_question_avg WHERE question_type != 'single_select')
    GROUP BY wave_id`, */
  form: `
    SELECT 
      (SELECT MAX(wave_id) FROM wave_question_avg WHERE form_id = ? AND question_type != 'single_select') AS wave_id,
      ROUND(AVG(delta_vs_prev_avg), 2) AS delta_vs_prev_avg
    FROM wave_question_avg
    WHERE form_id = ?
      AND question_type != 'single_select'
      AND wave_id = (SELECT MAX(wave_id) FROM wave_question_avg WHERE form_id = ? AND question_type != 'single_select')
    GROUP BY wave_id`,
  form_wave: `
    SELECT wave_id, ROUND(AVG(delta_vs_prev_avg), 2) AS delta_vs_prev_avg
    FROM wave_question_avg
    WHERE form_id = ? AND wave_id = ? AND question_type != 'single_select'
    GROUP BY wave_id`,
};
export async function getTrend(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  let finalParams = params;
  if (suffix === "form") {
    const [form_id] = params;
    finalParams = [form_id, form_id, form_id];
    const sql = SQL_TREND[suffix];
    const [rows] = await pool.query(sql, finalParams);
    return rows;
  } else if (suffix === "form_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const segment_id = params[1];
    //console.log(segment_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company} AS fdc
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 24
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    //console.log(sql);
    const [rows] = await pool.query(sql, [segment_id, company, form_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const wave_id = params[1];
    //console.log(wave_id);
    const segment_id = params[2];
    //console.log(segment_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };
    // Optional: validate `company` if it's ever user-influenced
    const sql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company} AS fdc
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu
                      ON csu.segment_id = ?            -- e.g. 23
                    AND csu.company = ?               -- e.g. 2
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    WHERE fdc.form_id = ?              -- e.g. 24
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company,
      form_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  } else {
    const sql = SQL_TREND[suffix];
    const [rows] = await pool.query(sql, params);
    return rows;
  }
}

/** 9) Respondents Data (dynamic table name via company lookup) */
export async function getRespondentsData(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "form") {
    const form_id = params[0];
    //console.log(form_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };

    const sql = `
    SELECT 
      fdc.respondent_id AS id, 
      cu.name, 
      ROUND(AVG(fdc.option_numeric),2) AS rag_score_avg
    FROM form_data_company${company} AS fdc
    LEFT JOIN client_users cu ON fdc.respondent_id = cu.id
    WHERE fdc.form_id = ? 
      AND fdc.option_numeric REGEXP '^[0-9]+$'
    GROUP BY fdc.respondent_id
    ORDER BY fdc.respondent_id ASC
  `;
    const [rows] = await pool.query(sql, [form_id]);
    return { result: rows };
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    //console.log(form_id);
    const wave_id = params[1];
    //console.log(wave_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };

    const sql = `
            SELECT 
                  fdc.respondent_id AS id, 
                  cu.name, 
                --  fdc.wave_id,
                ROUND(AVG(fdc.option_numeric),2) AS rag_score_avg
                FROM form_data_company${company} AS fdc
                LEFT JOIN client_users cu ON fdc.respondent_id = cu.id
                WHERE fdc.form_id = ? 
                  AND fdc.option_numeric REGEXP '^[0-9]+$'
                AND fdc.wave_id = ?
                    GROUP BY fdc.respondent_id
                ORDER BY fdc.respondent_id ASC
              `;
    const [rows] = await pool.query(sql, [form_id, wave_id]);
    return { result: rows };
  } else if (suffix === "form_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const segment_id = params[1];
    //console.log(segment_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };

    const sql = `SELECT
                    fdc.respondent_id AS id,
                    cu.name,
                    ROUND(AVG(fdc.option_numeric), 2) AS rag_score_avg
                  FROM form_data_company${company} AS fdc
                  LEFT JOIN client_users cu
                    ON cu.id = fdc.respondent_id
                  INNER JOIN client_segment_users csu
                    ON csu.company   = ?
                  AND csu.segment_id = ?
                  AND FIND_IN_SET(
                        fdc.respondent_id,
                        REPLACE(COALESCE(csu.client_users, ''), ' ', '')  -- strip spaces like "1, 2,3"
                      ) > 0
                  WHERE fdc.form_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                  GROUP BY fdc.respondent_id, cu.name
                  ORDER BY fdc.respondent_id ASC
                  `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [company, segment_id, form_id]);
    //console.log(rows);
    return { result: rows };
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    //console.log(form_id);
    const wave_id = params[1];
    //console.log(wave_id);
    const segment_id = params[2];
    //console.log(segment_id);
    const [companyRows] = await pool.query(
      `SELECT company FROM user_stakeholder_form WHERE id = ?`,
      [form_id]
    );
    const company = companyRows?.[0]?.company;
    if (!company) return { result: [] };

    const sql = `SELECT
                    fdc.respondent_id AS id,
                    cu.name,
                    ROUND(AVG(fdc.option_numeric), 2) AS rag_score_avg
                  FROM form_data_company${company} AS fdc
                  LEFT JOIN client_users cu
                    ON cu.id = fdc.respondent_id
                  INNER JOIN client_segment_users csu
                    ON csu.company   = ?
                  AND csu.segment_id = ?
                  AND FIND_IN_SET(
                        fdc.respondent_id,
                        REPLACE(COALESCE(csu.client_users, ''), ' ', '')  -- strip spaces like "1, 2,3"
                      ) > 0
                  WHERE fdc.form_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    AND fdc.wave_id = ?
                  GROUP BY fdc.respondent_id, cu.name
                  ORDER BY fdc.respondent_id ASC
                  `;
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      company,
      segment_id,
      form_id,
      wave_id,
    ]);
    //console.log(rows);
    return { result: rows };
  }
}
