import pool from "./db";
import { parseQueryParams } from "./utils/parseQueryParams";

export default async (req, res, next) => {
  //other users show only their own questions
  if (userRole != "1" && userRole != "6") {
    whereClauses.push("(q.company = ?)");
    values.push(parentId ? parentId : userId);
  }

  if (userRole != "1" && userRole != "6") {
    //show all data where company is null
    //and also check for user.parent_id , if its null then use userId else use parentId
    whereClauses.push("(q.company IS NULL OR q.company = ?)");
    values.push(parentId ? parentId : userId);
  }
};

//Business---------------------------------------------------------
// Team: Sales Team
// Company: NiftyAI

//Personal---------------------------------------------------------
// Name: Victoria Metrics
// Title: Vice President, Portfolio Intelligence
// Company: NiftyAI

/* return res
        .status(500)
        .json({ message: "Internal Server Error", error: error.message }); */

/* WITH base AS (
  SELECT
    fdc.form_id,
    fdc.wave_id,
    fdc.question_id,
    q.three_word_outcome_en,
    q.three_word_outcome_nl,
    AVG(fdc.option_numeric) AS avg_score
  FROM form_data_company${company} AS fdc
  LEFT JOIN question q
    ON q.id = fdc.question_id
  INNER JOIN client_segment_users AS csu
    ON csu.segment_id = 23            -- e.g. 23
   AND csu.company = 2               -- e.g. 2
   AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
  WHERE fdc.form_id = 24              -- e.g. 24
    AND fdc.option_numeric REGEXP '^[0-9]+$'
  GROUP BY fdc.wave_id, fdc.question_id, q.three_word_outcome_en, q.three_word_outcome_nl
)
SELECT
form_id,
  wave_id,
  question_id,
  three_word_outcome_en,
  three_word_outcome_nl,
  ROUND(avg_score, 2) AS avg_score,
  ROUND(
    avg_score - LAG(avg_score) OVER (PARTITION BY question_id ORDER BY wave_id),
    2
  ) AS diff_vs_prev
FROM base
ORDER BY question_id, wave_id; */

// ✅ Disable pagination even if present
/*     options.limit = undefined;
    options.offset = undefined; */
/* AND FIND_IN_SET(
fdc.respondent_id,
REPLACE(COALESCE(csu.client_users, ''), ' ', '')  -- strip spaces like "1, 2,3"
) > 0 */

/* const created_at = req.query.created_at
  ? req.query.created_at.split(",")  // ['2025-01-01', '2025-12-31']
  : undefined;

let createdAtCondition = "";

if (created_at && created_at.length === 2) {
  const [startDate, endDate] = created_at;
  createdAtCondition = `fdc.created_at BETWEEN '${startDate}' AND '${endDate}'`;
}

console.log("createdAtCondition", createdAtCondition); */

//applyExtraFilters starts______________________________________
function applyExtraFilters(baseSql, filters, params) {
  let whereParts = [];
  let extraParams = [];
  console.log("Filters in applyExtraFilters:", filters);

  if (
    filters.created_at &&
    typeof filters.created_at === "string" &&
    filters.created_at.includes("BETWEEN")
  ) {
    console.log("Applying created_at filter with value:", filters.created_at);
    whereParts.push(filters.created_at);
  }

  // Example: region filter
  // if (filters.region_id) {
  //   whereParts.push("fdc.region_id = ?");
  //   extraParams.push(filters.region_id);
  // }

  // Example: department filter
  // if (filters.department_id) {
  //   whereParts.push("fdc.department_id = ?");
  //   extraParams.push(filters.department_id);
  // }

  // Inject extra WHERE parts before GROUP BY
  let sql = baseSql;
  if (whereParts.length > 0) {
    console.log("Applying extra WHERE conditions:", whereParts);
    sql = sql.replace("/*EXTRA_FILTERS*/", " AND " + whereParts.join(" AND "));
    // return { sql, params: [...params, ...extraParams] };
    return { sql, params: [...params] };
  } else {
    sql = sql.replace("/*EXTRA_FILTERS*/", "");
    return { sql, params };
  }
}
let queryParams = [form_id]; // base params
const { sql, params: finalParams } = applyExtraFilters(
  sqlBase,
  filters,
  queryParams
);
console.log("Final SQL:", sql);
console.log("Final Params:", finalParams);
const [rows] = await pool.query(sql, finalParams);
//applyExtraFilters ends_____________________________________
//
//question view starts+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

// Split helper for "response_options_en" that may be delimited by | , ; or tabs
const splitLabels = (s) =>
  String(s || "")
    .split(/[\|\t;,]+/g)
    .map((t) => t.trim())
    .filter(Boolean);
//

/** 15) Response Percent (requires form_id) */
export async function getResponsePercent(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "form") {
    const form_id = params[0];
    const [responsesRows] = await pool.query(
      `SELECT SUM(total_users) AS total_users_sum
     FROM ( SELECT total_users
            FROM wave_question_avg
            WHERE form_id = ?
            GROUP BY wave_id ) AS grouped_data`,
      [form_id]
    );

    const [formsSentRows] = await pool.query(
      `SELECT SUM(tot_email_sent) as tot_email_sent
     FROM user_stakeholder_form_logs
     WHERE user_stakeholder_form_id = ?`,
      [form_id]
    );

    const totalResponses = responsesRows?.[0]?.total_users_sum || 0;
    const totalSent = formsSentRows?.[0]?.tot_email_sent || 0;
    const responsePercentage =
      totalSent > 0
        ? Number(((totalResponses / totalSent) * 100).toFixed(2))
        : 0;
    return { totalResponses, totalSent, responsePercentage };
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const wave_id = params[1];
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
        ? Number(((totalResponses / totalSent) * 100).toFixed(2))
        : 0;

    return { totalResponses, totalSent, responsePercentage };
  } else if (suffix === "form_segment") {
    const form_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];

    const [responsesRows] = await pool.query(
      `WITH base AS (
    		SELECT
            fdc.form_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
          FROM form_data_company${company_id} AS fdc
            INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?
                AND csu.company = ?
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
          WHERE fdc.form_id = ?
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
           )
           SELECT form_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`,
      [segment_id, company_id, form_id]
    );
    ////////////////////////////////////////////////////////////////////
    const breakdown = await getWaveSegmentDelivery({
      company_id,
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
        ? Number(((totalResponses / totalSent) * 100).toFixed(2))
        : 0;

    return { totalResponses, totalSent, responsePercentage };
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];

    const [responsesRows] = await pool.query(
      `SELECT
            fdc.form_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
          FROM form_data_company${company_id} AS fdc
            INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?
                AND csu.company = ?
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
          WHERE fdc.form_id = ? AND fdc.wave_id=?
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC`,
      [segment_id, company_id, form_id, wave_id]
    );
    ////////////////////////////////////////////////////////////////////
    const breakdown = await getWaveSegmentDelivery({
      company_id,
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
        ? Number(((totalResponses / totalSent) * 100).toFixed(2))
        : 0;

    return { totalResponses, totalSent, responsePercentage };
  }
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

//getWaveSegmentDelivery starts++++++++++++++++++++++++++++++++
/**
 * Get per-wave counts of sent IDs that belong to a segment's client_users.
 * Returns an array of rows, one per wave.
 */
export async function getWaveSegmentDelivery({
  company_id,
  segment_id,
  form_id,
}) {
  // A) segment users
  const [segRows] = await pool.query(
    `SELECT client_users FROM client_segment_users WHERE company = ? AND segment_id = ?`,
    [company_id, segment_id]
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
      company: Number(company_id),
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
      company: Number(company_id),
      segment_id: Number(segment_id),
      wave: Number(r.wave),
      sent_in_segment: uniqMatched.length,
      total_sent: sentSet.size,
      segment_user_count: segmentUsersList.length,
      matched_user_ids: uniqMatched.join(","),
    };
  });
}
//getWaveSegmentDelivery ends+++++++++++++++++++++++++++++++++++
//
export async function getResponsePercent(filters) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "form") {
    const form_id = params[0];
    const question_id = params[1];
    const [responsesRows] = await pool.query(
      `SELECT SUM(total_users) AS total_users_sum 
     FROM ( SELECT total_users 
            FROM wave_question_avg 
            WHERE form_id = ? AND question_id = ?
            GROUP BY wave_id ) AS grouped_data`,
      [form_id, question_id]
    );
    const totalResponses = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponses };
  } else if (suffix == "form_wave") {
    const form_id = params[0];
    const question_id = params[1];
    const wave_id = params[2];
    const [responsesRows] = await pool.query(
      `SELECT form_id, wave_id, total_users FROM \`wave_question_avg\` where form_id=? AND question_id = ? AND wave_id=? LIMIT 1`,
      [form_id, question_id, wave_id]
    );
    const totalResponses = responsesRows?.[0]?.total_users || 0;

    return { totalResponses };
  } else if (suffix === "form_segment") {
    const form_id = params[0];
    const question_id = params[1];
    const segment_id = params[2];
    const company = params[3];
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
          WHERE fdc.form_id = ? AND fdc.question_id = ?
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
           )
           SELECT form_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`,
      [segment_id, company, form_id, question_id]
    );
    const totalResponses = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponses };
  } else if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const question_id = params[1];
    const wave_id = params[2];
    const segment_id = params[3];
    const company = params[4];
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
          WHERE fdc.form_id = ? AND fdc.wave_id=? AND fdc.question_id = ?
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC`,
      [segment_id, company, form_id, wave_id, question_id]
    );
    const totalResponses = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponses };
  }
}
export async function getWaveOptionDistributionWithLabels(filters) {
  const { suffix, params } = pickVariant(filters);
  const form_id = params[0];
  const question_id = params[1];
  //
  if (suffix === "form") {
    const company = params[2];
    const meta = await getQuestionMeta(question_id);
    if (!meta) throw new Error("Question not found");

    const type = String(meta.question_type_name || "").toLowerCase();
    console.log("type", type);
    // Single-select: 5 custom strings in the question row
    if (type.includes("single-select")) {
      const labels = splitLabels(meta.custom_options_en);
      meta["options"] = labels;
      if (labels.length !== 5)
        throw new Error("Expected 5 options for single-select");
      const sql = /* sql C from above */ `
      WITH base AS (
        SELECT fdc.wave_id, TRIM(fdc.option_numeric) AS opt
        FROM form_data_company${company} AS fdc
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric IS NOT NULL
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT ? AS opt, ? AS label
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.label;
    `;
      const paramss = [form_id, question_id, ...labels.flatMap((l) => [l, l])];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotSingleSelect(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }

    // Closed (Yes/No) → use first two labels from question_type
    if (type.startsWith("closed")) {
      const labels = splitLabels(meta.response_options_en); // e.g. ["Yes","No"]
      meta["options"] = labels;
      if (labels.length < 2) throw new Error("Expected 2 labels for Closed");
      const noLabel = labels.find((l) => /no/i.test(l)) || labels[1] || "No";
      const yesLabel = labels.find((l) => /yes/i.test(l)) || labels[0] || "Yes";

      const sql = /* sql B from above */ `
      WITH base AS (
        SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
        FROM form_data_company${company} AS fdc
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$'
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT 1 AS opt, ? AS label
        UNION ALL SELECT 5, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.opt;
    `;
      const paramss = [form_id, question_id, noLabel, yesLabel];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotClosedQuestion(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }
    // // Likert / Rating (1..5): use 5 labels from question_type
    if (type.includes("rating (generic 1-5)") || type.startsWith("likert")) {
      const labels = splitLabels(meta.response_options_en);
      meta["options"] = labels;
      if (labels.length < 5) {
        throw new Error("Expected 5 labels for Likert/Rating");
      } else {
        const sql = /* sql A from above */ `
          WITH base AS (
            SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
            FROM form_data_company${company} AS fdc
            WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$'
          ),
          totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
          opts AS (
            SELECT 1 AS opt, ? AS label
            UNION ALL SELECT 2, ?
            UNION ALL SELECT 3, ?
            UNION ALL SELECT 4, ?
            UNION ALL SELECT 5, ?
          ),
          waves AS ( SELECT DISTINCT wave_id FROM base ),
          grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
          counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
          SELECT
            g.wave_id,
            g.opt AS option_value,
            g.label AS option_label,
            COALESCE(c.cnt, 0) AS count_per_option,
            ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
          FROM grid g
          LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
          JOIN totals t ON t.wave_id = g.wave_id
          ORDER BY g.wave_id, g.opt;
        `;
        const paramss = [
          form_id,
          question_id,
          labels[0],
          labels[1],
          labels[2],
          labels[3],
          labels[4],
        ];
        const [rows] = await pool.query(sql, paramss);
        //question_type_name": "Rating (Generic 1-5)"
        if (type.includes("rating (generic 1-5)")) {
          //console.log("Rating (Generic 1-5)");
          //const pivoted = pivotRatingQuestion(rows); // rounded ints
          const pivoted = await pivotRatingQuestion(rows, { round: false }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        } else if (type.startsWith("likert")) {
          //console.log("likert");
          //const pivoted = pivotLikertQuestion(rows); // rounded ints
          const pivoted = await pivotLikertQuestion(rows, meta.options, {
            round: false,
          }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        }
      }
    }
  } else if (suffix == "form_wave") {
    const wave_id = params[2];
    const company = params[3];
    const meta = await getQuestionMeta(question_id);
    if (!meta) throw new Error("Question not found");

    const type = String(meta.question_type_name || "").toLowerCase();
    console.log("type", type);
    // Single-select: 5 custom strings in the question row
    if (type.includes("single-select")) {
      const labels = splitLabels(meta.custom_options_en);
      meta["options"] = labels;
      if (labels.length !== 5)
        throw new Error("Expected 5 options for single-select");
      const sql = /* sql C from above */ `
      WITH base AS (
        SELECT fdc.wave_id, TRIM(fdc.option_numeric) AS opt
        FROM form_data_company${company} AS fdc
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric IS NOT NULL AND fdc.wave_id = ?
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT ? AS opt, ? AS label
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.label;
    `;
      const paramss = [
        form_id,
        question_id,
        wave_id,
        ...labels.flatMap((l) => [l, l]),
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotSingleSelect(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }

    // Closed (Yes/No) → use first two labels from question_type
    if (type.startsWith("closed")) {
      const labels = splitLabels(meta.response_options_en); // e.g. ["Yes","No"]
      meta["options"] = labels;
      if (labels.length < 2) throw new Error("Expected 2 labels for Closed");
      const noLabel = labels.find((l) => /no/i.test(l)) || labels[1] || "No";
      const yesLabel = labels.find((l) => /yes/i.test(l)) || labels[0] || "Yes";

      const sql = /* sql B from above */ `
      WITH base AS (
        SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
        FROM form_data_company${company} AS fdc
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$' AND fdc.wave_id = ?
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT 1 AS opt, ? AS label
        UNION ALL SELECT 5, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.opt;
    `;
      const paramss = [form_id, question_id, wave_id, noLabel, yesLabel];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotClosedQuestion(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }
    // // Likert / Rating (1..5): use 5 labels from question_type
    if (type.includes("rating (generic 1-5)") || type.startsWith("likert")) {
      const labels = splitLabels(meta.response_options_en);
      meta["options"] = labels;
      if (labels.length < 5) {
        throw new Error("Expected 5 labels for Likert/Rating");
      } else {
        const sql = /* sql A from above */ `
          WITH base AS (
            SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
            FROM form_data_company${company} AS fdc
            WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$' AND fdc.wave_id = ?
          ),
          totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
          opts AS (
            SELECT 1 AS opt, ? AS label
            UNION ALL SELECT 2, ?
            UNION ALL SELECT 3, ?
            UNION ALL SELECT 4, ?
            UNION ALL SELECT 5, ?
          ),
          waves AS ( SELECT DISTINCT wave_id FROM base ),
          grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
          counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
          SELECT
            g.wave_id,
            g.opt AS option_value,
            g.label AS option_label,
            COALESCE(c.cnt, 0) AS count_per_option,
            ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
          FROM grid g
          LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
          JOIN totals t ON t.wave_id = g.wave_id
          ORDER BY g.wave_id, g.opt;
        `;
        const paramss = [
          form_id,
          question_id,
          wave_id,
          labels[0],
          labels[1],
          labels[2],
          labels[3],
          labels[4],
        ];
        const [rows] = await pool.query(sql, paramss);
        //question_type_name": "Rating (Generic 1-5)"
        if (type.includes("rating (generic 1-5)")) {
          //console.log("Rating (Generic 1-5)");
          //const pivoted = pivotRatingQuestion(rows); // rounded ints
          const pivoted = await pivotRatingQuestion(rows, { round: false }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        } else if (type.startsWith("likert")) {
          //console.log("likert");
          //const pivoted = pivotLikertQuestion(rows); // rounded ints
          const pivoted = await pivotLikertQuestion(rows, meta.options, {
            round: false,
          }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        }
      }
    }
  } else if (suffix == "form_segment") {
    const segment_id = params[2];
    const company = params[3];
    const meta = await getQuestionMeta(question_id);
    if (!meta) throw new Error("Question not found");

    const type = String(meta.question_type_name || "").toLowerCase();
    console.log("type", type);
    // Single-select: 5 custom strings in the question row
    if (type.includes("single-select")) {
      const labels = splitLabels(meta.custom_options_en);
      meta["options"] = labels;
      if (labels.length !== 5)
        throw new Error("Expected 5 options for single-select");
      const sql = /* sql C from above */ `
      WITH base AS (
        SELECT fdc.wave_id, TRIM(fdc.option_numeric) AS opt
        FROM form_data_company${company} AS fdc
        INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric IS NOT NULL
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT ? AS opt, ? AS label
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.label;
    `;
      const paramss = [
        segment_id,
        company,
        form_id,
        question_id,
        ...labels.flatMap((l) => [l, l]),
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotSingleSelect(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }

    // Closed (Yes/No) → use first two labels from question_type
    if (type.startsWith("closed")) {
      const labels = splitLabels(meta.response_options_en); // e.g. ["Yes","No"]
      meta["options"] = labels;
      if (labels.length < 2) throw new Error("Expected 2 labels for Closed");
      const noLabel = labels.find((l) => /no/i.test(l)) || labels[1] || "No";
      const yesLabel = labels.find((l) => /yes/i.test(l)) || labels[0] || "Yes";

      const sql = /* sql B from above */ `
      WITH base AS (
        SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
        FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$'
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT 1 AS opt, ? AS label
        UNION ALL SELECT 5, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.opt;
    `;
      const paramss = [
        segment_id,
        company,
        form_id,
        question_id,
        noLabel,
        yesLabel,
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotClosedQuestion(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }
    // // Likert / Rating (1..5): use 5 labels from question_type
    if (type.includes("rating (generic 1-5)") || type.startsWith("likert")) {
      const labels = splitLabels(meta.response_options_en);
      meta["options"] = labels;
      if (labels.length < 5) {
        throw new Error("Expected 5 labels for Likert/Rating");
      } else {
        const sql = /* sql A from above */ `
          WITH base AS (
            SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
            FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
            WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$'
          ),
          totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
          opts AS (
            SELECT 1 AS opt, ? AS label
            UNION ALL SELECT 2, ?
            UNION ALL SELECT 3, ?
            UNION ALL SELECT 4, ?
            UNION ALL SELECT 5, ?
          ),
          waves AS ( SELECT DISTINCT wave_id FROM base ),
          grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
          counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
          SELECT
            g.wave_id,
            g.opt AS option_value,
            g.label AS option_label,
            COALESCE(c.cnt, 0) AS count_per_option,
            ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
          FROM grid g
          LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
          JOIN totals t ON t.wave_id = g.wave_id
          ORDER BY g.wave_id, g.opt;
        `;
        const paramss = [
          segment_id,
          company,
          form_id,
          question_id,
          labels[0],
          labels[1],
          labels[2],
          labels[3],
          labels[4],
        ];
        const [rows] = await pool.query(sql, paramss);
        //question_type_name": "Rating (Generic 1-5)"
        if (type.includes("rating (generic 1-5)")) {
          //console.log("Rating (Generic 1-5)");
          //const pivoted = pivotRatingQuestion(rows); // rounded ints
          const pivoted = await pivotRatingQuestion(rows, { round: false }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        } else if (type.startsWith("likert")) {
          //console.log("likert");
          //const pivoted = pivotLikertQuestion(rows); // rounded ints
          const pivoted = await pivotLikertQuestion(rows, meta.options, {
            round: false,
          }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        }
      }
    }
  } else if (suffix == "form_wave_segment") {
    const wave_id = params[2];
    const segment_id = params[3];
    const company = params[4];
    const meta = await getQuestionMeta(question_id);
    if (!meta) throw new Error("Question not found");

    const type = String(meta.question_type_name || "").toLowerCase();
    console.log("type", type);
    // Single-select: 5 custom strings in the question row
    if (type.includes("single-select")) {
      const labels = splitLabels(meta.custom_options_en);
      meta["options"] = labels;
      if (labels.length !== 5)
        throw new Error("Expected 5 options for single-select");
      const sql = /* sql C from above */ `
      WITH base AS (
        SELECT fdc.wave_id, TRIM(fdc.option_numeric) AS opt
        FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric IS NOT NULL AND fdc.wave_id = ?
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT ? AS opt, ? AS label
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.label;
    `;
      const paramss = [
        segment_id,
        company,
        form_id,
        question_id,
        wave_id,
        ...labels.flatMap((l) => [l, l]),
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotSingleSelect(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }

    // Closed (Yes/No) → use first two labels from question_type
    if (type.startsWith("closed")) {
      const labels = splitLabels(meta.response_options_en); // e.g. ["Yes","No"]
      meta["options"] = labels;
      if (labels.length < 2) throw new Error("Expected 2 labels for Closed");
      const noLabel = labels.find((l) => /no/i.test(l)) || labels[1] || "No";
      const yesLabel = labels.find((l) => /yes/i.test(l)) || labels[0] || "Yes";

      const sql = /* sql B from above */ `
      WITH base AS (
        SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
        FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$' AND fdc.wave_id = ?
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT 1 AS opt, ? AS label
        UNION ALL SELECT 5, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.opt;
    `;
      const paramss = [
        segment_id,
        company,
        form_id,
        question_id,
        wave_id,
        noLabel,
        yesLabel,
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotClosedQuestion(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }
    // // Likert / Rating (1..5): use 5 labels from question_type
    if (type.includes("rating (generic 1-5)") || type.startsWith("likert")) {
      const labels = splitLabels(meta.response_options_en);
      meta["options"] = labels;
      if (labels.length < 5) {
        throw new Error("Expected 5 labels for Likert/Rating");
      } else {
        const sql = /* sql A from above */ `
          WITH base AS (
            SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
            FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
            WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$' AND fdc.wave_id = ?
          ),
          totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
          opts AS (
            SELECT 1 AS opt, ? AS label
            UNION ALL SELECT 2, ?
            UNION ALL SELECT 3, ?
            UNION ALL SELECT 4, ?
            UNION ALL SELECT 5, ?
          ),
          waves AS ( SELECT DISTINCT wave_id FROM base ),
          grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
          counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
          SELECT
            g.wave_id,
            g.opt AS option_value,
            g.label AS option_label,
            COALESCE(c.cnt, 0) AS count_per_option,
            ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
          FROM grid g
          LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
          JOIN totals t ON t.wave_id = g.wave_id
          ORDER BY g.wave_id, g.opt;
        `;
        const paramss = [
          segment_id,
          company,
          form_id,
          question_id,
          wave_id,
          labels[0],
          labels[1],
          labels[2],
          labels[3],
          labels[4],
        ];
        const [rows] = await pool.query(sql, paramss);
        //question_type_name": "Rating (Generic 1-5)"
        if (type.includes("rating (generic 1-5)")) {
          //console.log("Rating (Generic 1-5)");
          //const pivoted = pivotRatingQuestion(rows); // rounded ints
          const pivoted = await pivotRatingQuestion(rows, { round: false }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        } else if (type.startsWith("likert")) {
          //console.log("likert");
          //const pivoted = pivotLikertQuestion(rows); // rounded ints
          const pivoted = await pivotLikertQuestion(rows, meta.options, {
            round: false,
          }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        }
      }
    }
  }
}
export async function getHighestOptionPerc(filters) {
  const { suffix, params } = pickVariant(filters);
  const form_id = params[0];
  const question_id = params[1];

  // Assumes 'question' table links to 'question_type'
  if (suffix == "form") {
    const company = params[2];
    const sql = `
                  SELECT
                  form_id,question_id,
                    option_numeric,
                    COUNT(*) AS occurrences,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
                  FROM form_data_company${company}
                  WHERE form_id = ? AND question_id = ?
                  GROUP BY option_numeric
                  ORDER BY occurrences DESC LIMIT 1`;
    const [rows] = await pool.query(sql, [form_id, question_id]);
    return rows;
  } else if (suffix == "form_wave") {
    const wave_id = params[2];
    const company = params[3];
    const sql = `
        SELECT
            form_id,question_id,
              option_numeric,
              COUNT(*) AS occurrences,
              ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
            FROM form_data_company${company}
            WHERE form_id = ? AND question_id = ? AND wave_id = ?
            GROUP BY option_numeric
            ORDER BY occurrences DESC LIMIT 1`;
    const [rows] = await pool.query(sql, [form_id, question_id, wave_id]);
    return rows;
  } else if (suffix == "form_segment") {
    const segment_id = params[2];
    const company = params[3];
    const sql = `
        SELECT
            fdc.form_id, fdc.question_id, fdc.option_numeric,
              COUNT(*) AS occurrences,
              ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
            FROM form_data_company${company} AS fdc
              INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
            WHERE fdc.form_id = ? AND fdc.question_id = ?
            GROUP BY fdc.option_numeric
            ORDER BY occurrences DESC LIMIT 1`;
    const [rows] = await pool.query(sql, [
      segment_id,
      company,
      form_id,
      question_id,
    ]);
    return rows;
  } else if (suffix == "form_wave_segment") {
    const wave_id = params[2];
    const segment_id = params[3];
    const company = params[4];
    const sql = `
        SELECT
            fdc.form_id, fdc.question_id, fdc.option_numeric,
              COUNT(*) AS occurrences,
              ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
            FROM form_data_company${company} AS fdc
              INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
            WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.wave_id = ?
            GROUP BY fdc.option_numeric
            ORDER BY occurrences DESC LIMIT 1`;
    const [rows] = await pool.query(sql, [
      segment_id,
      company,
      form_id,
      question_id,
      wave_id,
    ]);
    return rows;
  }
}
async function pivotSingleSelect(rows) {
  // collect all waves (sorted numerically)
  const waves = [...new Set(rows.map((r) => Number(r.wave_id)))].sort(
    (a, b) => a - b
  );
  const waveKeys = waves.map((w) => `Wave${w}`);

  // map option_label -> { name, Wave1, Wave2, ... }
  const byOption = new Map();

  for (const r of rows) {
    const name = String(r.option_label || r.option_value || "").trim();
    if (!name) continue;

    if (!byOption.has(name)) {
      const base = { name };
      // init all waves with 0
      for (const w of waves) base[`Wave${w}`] = 0;
      byOption.set(name, base);
    }
    const obj = byOption.get(name);
    const key = `Wave${Number(r.wave_id)}`;
    //const pct = Math.round(Number(r.percentage)) || 0; // ensure number
    const pct = Number(r.percentage) || 0; // ensure number
    obj[key] = pct;
  }

  // return in a stable order (alphabetical by name)
  return Array.from(byOption.values()).sort((a, b) =>
    a.name.localeCompare(b.name)
  );
}
async function pivotClosedQuestion(rows) {
  const waves = [...new Set(rows.map((r) => Number(r.wave_id)))].sort(
    (a, b) => b - a
  );

  const result = [];
  for (const w of waves) {
    const waveRows = rows.filter((r) => Number(r.wave_id) === w);

    // normalize labels -> { yes: pct, no: pct }
    let yesPct = 0,
      noPct = 0;
    for (const r of waveRows) {
      const label = String(r.option_label ?? r.option_value ?? "")
        .trim()
        .toLowerCase();
      //const pct = Math.round(Number(r.percentage) || 0);
      const pct = Number(r.percentage) || 0; // keep 2-decimal strings as numbers
      if (label === "yes") yesPct = pct;
      else if (label === "no") noPct = pct;
    }

    // build in desired key order
    result.push({
      wave: `Wave ${w}`,
      Yes: yesPct,
      No: noPct,
    });
  }
  return result;
}
async function pivotRatingQuestion(rows, { round = true } = {}) {
  // numeric sort for waves
  const waves = [...new Set(rows.map((r) => Number(r.wave_id)))].sort(
    (a, b) => a - b
  );
  const waveKeys = waves.map((w) => `Wave${w}`);

  // ensure labels 1..5
  const labels = ["1", "2", "3", "4", "5"];
  const byLabel = new Map(
    labels.map((l) => {
      const base = { label: l };
      for (const wk of waveKeys) base[wk] = 0;
      return [l, base];
    })
  );

  for (const r of rows) {
    const label = String(r.option_label ?? r.option_value ?? "").trim();
    const waveKey = `Wave${Number(r.wave_id)}`;
    const pctNum = Number(r.percentage) || 0;
    const val = round ? Math.round(pctNum) : pctNum; // choose rounding
    // Only fill known labels 1..5; ignore stray values
    if (byLabel.has(label)) {
      byLabel.get(label)[waveKey] = val;
    }
  }

  // return in fixed label order 1..5
  return labels.map((l) => byLabel.get(l));
}
async function pivotLikertQuestion(rows, options, { round = true } = {}) {
  const waves = [...new Set(rows.map((r) => Number(r.wave_id)))].sort(
    (a, b) => b - a
  );
  const optionOrder = options;

  return waves.map((w) => {
    const waveRows = rows.filter((r) => Number(r.wave_id) === w);
    const obj = { wave: `Wave ${w}` };

    for (const opt of optionOrder) {
      const found = waveRows.find(
        (r) => (r.option_label ?? "").toLowerCase() === opt.toLowerCase()
      );
      const pct = found ? Number(found.percentage) : 0;
      obj[opt] = round ? Math.round(pct) : pct;
    }
    return obj;
  });
}
export async function getWaveOptionDistributionWithLabels(filters) {
  const { suffix, params } = pickVariant(filters);
  const form_id = params[0];
  const question_id = params[1];
  //
  if (suffix === "form") {
    const company = params[2];
    const meta = await getQuestionMeta(question_id);
    if (!meta) throw new Error("Question not found");

    const type = String(meta.question_type_name || "").toLowerCase();
    console.log("type", type);
    // Single-select: 5 custom strings in the question row
    if (type.includes("single-select")) {
      const labels = splitLabels(meta.custom_options_en);
      meta["options"] = labels;
      if (labels.length !== 5)
        throw new Error("Expected 5 options for single-select");
      const sql = /* sql C from above */ `
      WITH base AS (
        SELECT fdc.wave_id, TRIM(fdc.option_numeric) AS opt
        FROM form_data_company${company} AS fdc
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric IS NOT NULL
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT ? AS opt, ? AS label
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.label;
    `;
      const paramss = [form_id, question_id, ...labels.flatMap((l) => [l, l])];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotSingleSelect(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }

    // Closed (Yes/No) → use first two labels from question_type
    if (type.startsWith("closed")) {
      const labels = splitLabels(meta.response_options_en); // e.g. ["Yes","No"]
      meta["options"] = labels;
      if (labels.length < 2) throw new Error("Expected 2 labels for Closed");
      const noLabel = labels.find((l) => /no/i.test(l)) || labels[1] || "No";
      const yesLabel = labels.find((l) => /yes/i.test(l)) || labels[0] || "Yes";

      const sql = /* sql B from above */ `
      WITH base AS (
        SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
        FROM form_data_company${company} AS fdc
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$'
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT 1 AS opt, ? AS label
        UNION ALL SELECT 5, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.opt;
    `;
      const paramss = [form_id, question_id, noLabel, yesLabel];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotClosedQuestion(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }
    // // Likert / Rating (1..5): use 5 labels from question_type
    if (type.includes("rating (generic 1-5)") || type.startsWith("likert")) {
      const labels = splitLabels(meta.response_options_en);
      meta["options"] = labels;
      if (labels.length < 5) {
        throw new Error("Expected 5 labels for Likert/Rating");
      } else {
        const sql = /* sql A from above */ `
          WITH base AS (
            SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
            FROM form_data_company${company} AS fdc
            WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$'
          ),
          totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
          opts AS (
            SELECT 1 AS opt, ? AS label
            UNION ALL SELECT 2, ?
            UNION ALL SELECT 3, ?
            UNION ALL SELECT 4, ?
            UNION ALL SELECT 5, ?
          ),
          waves AS ( SELECT DISTINCT wave_id FROM base ),
          grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
          counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
          SELECT
            g.wave_id,
            g.opt AS option_value,
            g.label AS option_label,
            COALESCE(c.cnt, 0) AS count_per_option,
            ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
          FROM grid g
          LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
          JOIN totals t ON t.wave_id = g.wave_id
          ORDER BY g.wave_id, g.opt;
        `;
        const paramss = [
          form_id,
          question_id,
          labels[0],
          labels[1],
          labels[2],
          labels[3],
          labels[4],
        ];
        const [rows] = await pool.query(sql, paramss);
        //question_type_name": "Rating (Generic 1-5)"
        if (type.includes("rating (generic 1-5)")) {
          //console.log("Rating (Generic 1-5)");
          //const pivoted = pivotRatingQuestion(rows); // rounded ints
          const pivoted = await pivotRatingQuestion(rows, { round: false }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        } else if (type.startsWith("likert")) {
          //console.log("likert");
          //const pivoted = pivotLikertQuestion(rows); // rounded ints
          const pivoted = await pivotLikertQuestion(rows, meta.options, {
            round: false,
          }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        }
      }
    }
  } else if (suffix == "form_wave") {
    const wave_id = params[2];
    const company = params[3];
    const meta = await getQuestionMeta(question_id);
    if (!meta) throw new Error("Question not found");

    const type = String(meta.question_type_name || "").toLowerCase();
    console.log("type", type);
    // Single-select: 5 custom strings in the question row
    if (type.includes("single-select")) {
      const labels = splitLabels(meta.custom_options_en);
      meta["options"] = labels;
      if (labels.length !== 5)
        throw new Error("Expected 5 options for single-select");
      const sql = /* sql C from above */ `
      WITH base AS (
        SELECT fdc.wave_id, TRIM(fdc.option_numeric) AS opt
        FROM form_data_company${company} AS fdc
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric IS NOT NULL AND fdc.wave_id = ?
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT ? AS opt, ? AS label
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.label;
    `;
      const paramss = [
        form_id,
        question_id,
        wave_id,
        ...labels.flatMap((l) => [l, l]),
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotSingleSelect(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }

    // Closed (Yes/No) → use first two labels from question_type
    if (type.startsWith("closed")) {
      const labels = splitLabels(meta.response_options_en); // e.g. ["Yes","No"]
      meta["options"] = labels;
      if (labels.length < 2) throw new Error("Expected 2 labels for Closed");
      const noLabel = labels.find((l) => /no/i.test(l)) || labels[1] || "No";
      const yesLabel = labels.find((l) => /yes/i.test(l)) || labels[0] || "Yes";

      const sql = /* sql B from above */ `
      WITH base AS (
        SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
        FROM form_data_company${company} AS fdc
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$' AND fdc.wave_id = ?
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT 1 AS opt, ? AS label
        UNION ALL SELECT 5, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.opt;
    `;
      const paramss = [form_id, question_id, wave_id, noLabel, yesLabel];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotClosedQuestion(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }
    // // Likert / Rating (1..5): use 5 labels from question_type
    if (type.includes("rating (generic 1-5)") || type.startsWith("likert")) {
      const labels = splitLabels(meta.response_options_en);
      meta["options"] = labels;
      if (labels.length < 5) {
        throw new Error("Expected 5 labels for Likert/Rating");
      } else {
        const sql = /* sql A from above */ `
          WITH base AS (
            SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
            FROM form_data_company${company} AS fdc
            WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$' AND fdc.wave_id = ?
          ),
          totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
          opts AS (
            SELECT 1 AS opt, ? AS label
            UNION ALL SELECT 2, ?
            UNION ALL SELECT 3, ?
            UNION ALL SELECT 4, ?
            UNION ALL SELECT 5, ?
          ),
          waves AS ( SELECT DISTINCT wave_id FROM base ),
          grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
          counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
          SELECT
            g.wave_id,
            g.opt AS option_value,
            g.label AS option_label,
            COALESCE(c.cnt, 0) AS count_per_option,
            ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
          FROM grid g
          LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
          JOIN totals t ON t.wave_id = g.wave_id
          ORDER BY g.wave_id, g.opt;
        `;
        const paramss = [
          form_id,
          question_id,
          wave_id,
          labels[0],
          labels[1],
          labels[2],
          labels[3],
          labels[4],
        ];
        const [rows] = await pool.query(sql, paramss);
        //question_type_name": "Rating (Generic 1-5)"
        if (type.includes("rating (generic 1-5)")) {
          //console.log("Rating (Generic 1-5)");
          //const pivoted = pivotRatingQuestion(rows); // rounded ints
          const pivoted = await pivotRatingQuestion(rows, { round: false }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        } else if (type.startsWith("likert")) {
          //console.log("likert");
          //const pivoted = pivotLikertQuestion(rows); // rounded ints
          const pivoted = await pivotLikertQuestion(rows, meta.options, {
            round: false,
          }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        }
      }
    }
  } else if (suffix == "form_segment") {
    const segment_id = params[2];
    const company = params[3];
    const meta = await getQuestionMeta(question_id);
    if (!meta) throw new Error("Question not found");

    const type = String(meta.question_type_name || "").toLowerCase();
    console.log("type", type);
    // Single-select: 5 custom strings in the question row
    if (type.includes("single-select")) {
      const labels = splitLabels(meta.custom_options_en);
      meta["options"] = labels;
      if (labels.length !== 5)
        throw new Error("Expected 5 options for single-select");
      const sql = /* sql C from above */ `
      WITH base AS (
        SELECT fdc.wave_id, TRIM(fdc.option_numeric) AS opt
        FROM form_data_company${company} AS fdc
        INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric IS NOT NULL
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT ? AS opt, ? AS label
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.label;
    `;
      const paramss = [
        segment_id,
        company,
        form_id,
        question_id,
        ...labels.flatMap((l) => [l, l]),
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotSingleSelect(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }

    // Closed (Yes/No) → use first two labels from question_type
    if (type.startsWith("closed")) {
      const labels = splitLabels(meta.response_options_en); // e.g. ["Yes","No"]
      meta["options"] = labels;
      if (labels.length < 2) throw new Error("Expected 2 labels for Closed");
      const noLabel = labels.find((l) => /no/i.test(l)) || labels[1] || "No";
      const yesLabel = labels.find((l) => /yes/i.test(l)) || labels[0] || "Yes";

      const sql = /* sql B from above */ `
      WITH base AS (
        SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
        FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$'
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT 1 AS opt, ? AS label
        UNION ALL SELECT 5, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.opt;
    `;
      const paramss = [
        segment_id,
        company,
        form_id,
        question_id,
        noLabel,
        yesLabel,
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotClosedQuestion(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }
    // // Likert / Rating (1..5): use 5 labels from question_type
    if (type.includes("rating (generic 1-5)") || type.startsWith("likert")) {
      const labels = splitLabels(meta.response_options_en);
      meta["options"] = labels;
      if (labels.length < 5) {
        throw new Error("Expected 5 labels for Likert/Rating");
      } else {
        const sql = /* sql A from above */ `
          WITH base AS (
            SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
            FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
            WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$'
          ),
          totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
          opts AS (
            SELECT 1 AS opt, ? AS label
            UNION ALL SELECT 2, ?
            UNION ALL SELECT 3, ?
            UNION ALL SELECT 4, ?
            UNION ALL SELECT 5, ?
          ),
          waves AS ( SELECT DISTINCT wave_id FROM base ),
          grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
          counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
          SELECT
            g.wave_id,
            g.opt AS option_value,
            g.label AS option_label,
            COALESCE(c.cnt, 0) AS count_per_option,
            ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
          FROM grid g
          LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
          JOIN totals t ON t.wave_id = g.wave_id
          ORDER BY g.wave_id, g.opt;
        `;
        const paramss = [
          segment_id,
          company,
          form_id,
          question_id,
          labels[0],
          labels[1],
          labels[2],
          labels[3],
          labels[4],
        ];
        const [rows] = await pool.query(sql, paramss);
        //question_type_name": "Rating (Generic 1-5)"
        if (type.includes("rating (generic 1-5)")) {
          //console.log("Rating (Generic 1-5)");
          //const pivoted = pivotRatingQuestion(rows); // rounded ints
          const pivoted = await pivotRatingQuestion(rows, { round: false }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        } else if (type.startsWith("likert")) {
          //console.log("likert");
          //const pivoted = pivotLikertQuestion(rows); // rounded ints
          const pivoted = await pivotLikertQuestion(rows, meta.options, {
            round: false,
          }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        }
      }
    }
  } else if (suffix == "form_wave_segment") {
    const wave_id = params[2];
    const segment_id = params[3];
    const company = params[4];
    const meta = await getQuestionMeta(question_id);
    if (!meta) throw new Error("Question not found");

    const type = String(meta.question_type_name || "").toLowerCase();
    console.log("type", type);
    // Single-select: 5 custom strings in the question row
    if (type.includes("single-select")) {
      const labels = splitLabels(meta.custom_options_en);
      meta["options"] = labels;
      if (labels.length !== 5)
        throw new Error("Expected 5 options for single-select");
      const sql = /* sql C from above */ `
      WITH base AS (
        SELECT fdc.wave_id, TRIM(fdc.option_numeric) AS opt
        FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric IS NOT NULL AND fdc.wave_id = ?
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT ? AS opt, ? AS label
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
        UNION ALL SELECT ?, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.label;
    `;
      const paramss = [
        segment_id,
        company,
        form_id,
        question_id,
        wave_id,
        ...labels.flatMap((l) => [l, l]),
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotSingleSelect(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }

    // Closed (Yes/No) → use first two labels from question_type
    if (type.startsWith("closed")) {
      const labels = splitLabels(meta.response_options_en); // e.g. ["Yes","No"]
      meta["options"] = labels;
      if (labels.length < 2) throw new Error("Expected 2 labels for Closed");
      const noLabel = labels.find((l) => /no/i.test(l)) || labels[1] || "No";
      const yesLabel = labels.find((l) => /yes/i.test(l)) || labels[0] || "Yes";

      const sql = /* sql B from above */ `
      WITH base AS (
        SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
        FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$' AND fdc.wave_id = ?
      ),
      totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
      opts AS (
        SELECT 1 AS opt, ? AS label
        UNION ALL SELECT 5, ?
      ),
      waves AS ( SELECT DISTINCT wave_id FROM base ),
      grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
      counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
      SELECT
        g.wave_id,
        g.opt AS option_value,
        g.label AS option_label,
        COALESCE(c.cnt, 0) AS count_per_option,
        ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
      FROM grid g
      LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
      JOIN totals t ON t.wave_id = g.wave_id
      ORDER BY g.wave_id, g.opt;
    `;
      const paramss = [
        segment_id,
        company,
        form_id,
        question_id,
        wave_id,
        noLabel,
        yesLabel,
      ];
      const [rows] = await pool.query(sql, paramss);
      //change structure of output
      const output = await pivotClosedQuestion(rows);
      //console.log(output);
      return { questionData: meta, chartData: output };
    }
    // // Likert / Rating (1..5): use 5 labels from question_type
    if (type.includes("rating (generic 1-5)") || type.startsWith("likert")) {
      const labels = splitLabels(meta.response_options_en);
      meta["options"] = labels;
      if (labels.length < 5) {
        throw new Error("Expected 5 labels for Likert/Rating");
      } else {
        const sql = /* sql A from above */ `
          WITH base AS (
            SELECT fdc.wave_id, CAST(fdc.option_numeric AS UNSIGNED) AS opt
            FROM form_data_company${company} AS fdc
                INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
            WHERE fdc.form_id = ? AND fdc.question_id = ? AND fdc.option_numeric REGEXP '^[0-9]+$' AND fdc.wave_id = ?
          ),
          totals AS ( SELECT wave_id, COUNT(*) AS total FROM base GROUP BY wave_id ),
          opts AS (
            SELECT 1 AS opt, ? AS label
            UNION ALL SELECT 2, ?
            UNION ALL SELECT 3, ?
            UNION ALL SELECT 4, ?
            UNION ALL SELECT 5, ?
          ),
          waves AS ( SELECT DISTINCT wave_id FROM base ),
          grid AS ( SELECT w.wave_id, o.opt, o.label FROM waves w CROSS JOIN opts o ),
          counts AS ( SELECT wave_id, opt, COUNT(*) AS cnt FROM base GROUP BY wave_id, opt )
          SELECT
            g.wave_id,
            g.opt AS option_value,
            g.label AS option_label,
            COALESCE(c.cnt, 0) AS count_per_option,
            ROUND(COALESCE(c.cnt, 0) * 100.0 / NULLIF(t.total, 0), 2) AS percentage
          FROM grid g
          LEFT JOIN counts c ON c.wave_id = g.wave_id AND c.opt = g.opt
          JOIN totals t ON t.wave_id = g.wave_id
          ORDER BY g.wave_id, g.opt;
        `;
        const paramss = [
          segment_id,
          company,
          form_id,
          question_id,
          wave_id,
          labels[0],
          labels[1],
          labels[2],
          labels[3],
          labels[4],
        ];
        const [rows] = await pool.query(sql, paramss);
        //question_type_name": "Rating (Generic 1-5)"
        if (type.includes("rating (generic 1-5)")) {
          //console.log("Rating (Generic 1-5)");
          //const pivoted = pivotRatingQuestion(rows); // rounded ints
          const pivoted = await pivotRatingQuestion(rows, { round: false }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        } else if (type.startsWith("likert")) {
          //console.log("likert");
          //const pivoted = pivotLikertQuestion(rows); // rounded ints
          const pivoted = await pivotLikertQuestion(rows, meta.options, {
            round: false,
          }); // exact decimals
          //console.log(pivoted);
          return { questionData: meta, chartData: pivoted };
        }
      }
    }
  }
}
//question view ends+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

//     const baseSql = `WITH
// -- Split client_users into rows
// c_ids AS (
//     SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(c.client_users, ',', n.n), ',', -1) AS id
//     FROM client_segment_users c
//     JOIN (
//         SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
//         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
//         UNION ALL SELECT 9 UNION ALL SELECT 10
//     ) n
//     ON n.n <= 1 + LENGTH(c.client_users) - LENGTH(REPLACE(c.client_users, ',', ''))
//     WHERE c.segment_id = ?
// ),

// -- Split total_email_sent into rows
// l_ids AS (
//     SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(g.total_email_sent, ',', m.m), ',', -1) AS id
//     FROM (
//         SELECT GROUP_CONCAT(DISTINCT sent_ids ORDER BY sent_ids) AS total_email_sent
//         FROM user_stakeholder_form_logs
//         WHERE user_stakeholder_form_id = ?
//     ) g
//     JOIN (
//         SELECT 1 m UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
//         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
//         UNION ALL SELECT 9 UNION ALL SELECT 10
//     ) m
//     ON m.m <= 1 + LENGTH(g.total_email_sent) - LENGTH(REPLACE(g.total_email_sent, ',', ''))
// )

// -- Final intersection + aggregation
// SELECT COUNT(DISTINCT c_ids.id) AS total_email_sent
// FROM c_ids
// JOIN l_ids ON c_ids.id = l_ids.id;`;
//console.log(baseSql);

/* if (suffix == "form_wave_segment") {
    const form_id = params[0];
    const wave_id = params[1];
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH
-- Split client_users into rows
c_ids AS (
    SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(c.client_users, ',', n.n), ',', -1) AS id
    FROM client_segment_users c
    JOIN (
        SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10
    ) n
    ON n.n <= 1 + LENGTH(c.client_users) - LENGTH(REPLACE(c.client_users, ',', ''))
    WHERE c.segment_id = ?
),

-- Split total_email_sent into rows
l_ids AS (
    SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(g.total_email_sent, ',', m.m), ',', -1) AS id
    FROM (
        SELECT GROUP_CONCAT(DISTINCT sent_ids ORDER BY sent_ids) AS total_email_sent
        FROM user_stakeholder_form_logs
        WHERE user_stakeholder_form_id = ? AND wave = ?
    ) g
    JOIN (
        SELECT 1 m UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10
    ) m
    ON m.m <= 1 + LENGTH(g.total_email_sent) - LENGTH(REPLACE(g.total_email_sent, ',', ''))
)

-- Final intersection + aggregation
SELECT COUNT(DISTINCT c_ids.id) AS total_email_sent
FROM c_ids
JOIN l_ids ON c_ids.id = l_ids.id;`;
    //console.log(baseSql);
    const tableStr = `fdc`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [responsesRows] = await pool.query(sql, [
      segment_id,
      form_id,
      wave_id,
    ]);
    const formsSentRows = responsesRows?.[0]?.total_email_sent || 0;
    return { formsSentRows };
  } */

/**  Total Responses acc to filters */
// export async function getTotalResponsesFilters(filters) {
//   const { suffix, params } = pickVariant(filters);
//   //
//   if (suffix === "form") {
//     const form_id = params[0];
//     const company_id = params[1];
//     const baseSql = `WITH base AS (
//     		SELECT
//             fdc.form_id,
//             fdc.wave_id,
//             COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
//           FROM form_data_company${company_id} AS fdc
//            WHERE fdc.form_id = ?
//            /*EXTRA_FILTERS*/
//           GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
//            )
//            SELECT form_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`;
//     //console.log(baseSql);
//     const tableStr = `fdc`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [responsesRows] = await pool.query(sql, [form_id]);
//     // console.log(responsesRows);
//     const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
//     return { totalResponsesFilters };
//   } else if (suffix == "form_wave") {
//     const form_id = params[0];
//     const wave_id = params[1];
//     const company_id = params[2];
//     const baseSql = `SELECT
//             fdc.form_id,
//             fdc.wave_id,
//             COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
//           FROM form_data_company${company_id} AS fdc
//           WHERE fdc.form_id = ? AND fdc.wave_id=?
//           /*EXTRA_FILTERS*/
//           GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC`;
//     //console.log(baseSql);
//     const tableStr = `fdc`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [responsesRows] = await pool.query(sql, [form_id, wave_id]);
//     const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
//     return { totalResponsesFilters };
//   } else if (suffix === "form_segment") {
//     const form_id = params[0];
//     const segment_id = params[1];
//     const company_id = params[2];
//     const baseSql = `WITH base AS (
//     		SELECT
//             fdc.form_id,
//             fdc.wave_id,
//             COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
//           FROM form_data_company${company_id} AS fdc
//             INNER JOIN client_segment_users AS csu
//                 ON csu.segment_id = ?
//                 AND csu.company = ?
//                 AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//           WHERE fdc.form_id = ?
//           /*EXTRA_FILTERS*/
//           GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
//            )
//            SELECT form_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`;
//     //console.log(baseSql);
//     const tableStr = `fdc`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [responsesRows] = await pool.query(sql, [
//       segment_id,
//       company_id,
//       form_id,
//     ]);
//     const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
//     return { totalResponsesFilters };
//   } else if (suffix == "form_wave_segment") {
//     const form_id = params[0];
//     const wave_id = params[1];
//     const segment_id = params[2];
//     const company_id = params[3];
//     const baseSql = `SELECT
//             fdc.form_id,
//             fdc.wave_id,
//             COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
//           FROM form_data_company${company_id} AS fdc
//             INNER JOIN client_segment_users AS csu
//                 ON csu.segment_id = ?
//                 AND csu.company = ?
//                 AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//           WHERE fdc.form_id = ? AND fdc.wave_id=?
//           /*EXTRA_FILTERS*/
//           GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC`;
//     //console.log(baseSql);
//     const tableStr = `fdc`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [responsesRows] = await pool.query(sql, [
//       segment_id,
//       company_id,
//       form_id,
//       wave_id,
//     ]);
//     const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
//     return { totalResponsesFilters };
//   }
// }

/**  Total Responses For the form wdout filters */
// export async function getTotalResponses(form_id, company_id) {
//   const sql = `WITH base AS (
//     		SELECT
//             fdc.form_id,
//             fdc.wave_id,
//             COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
//           FROM form_data_company${company_id} AS fdc
//            WHERE fdc.form_id = ?
//           GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
//            )
//            SELECT form_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`;
//   //console.log(sql);
//   const [responsesRows] = await pool.query(sql, [form_id]);
//   const totalResponses = responsesRows?.[0]?.total_users_sum || 0;
//   return { totalResponses };
// }

/**  getFormsSentRows acc to filters */
// export async function getFormsSentRows(filters) {
//   const { suffix, params } = pickVariant(filters);
//   //
//   if (suffix === "form") {
//     const form_id = params[0];
//     const baseSql = `SELECT SUM(usfl.tot_email_sent) AS total_email_sent
//     FROM user_stakeholder_form_logs AS usfl
//     WHERE usfl.user_stakeholder_form_id = ?
//     /*EXTRA_FILTERS*/
//     GROUP BY usfl.user_stakeholder_form_id`;
//     // console.log(baseSql);
//     const tableStr = `fdc`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     // console.log(sql);
//     const [responsesRows] = await pool.query(sql, [form_id]);
//     console.log(responsesRows);
//     const formsSentRows = responsesRows?.[0]?.total_email_sent || 0;
//     return { formsSentRows };
//   } else if (suffix == "form_wave") {
//     const form_id = params[0];
//     const wave_id = params[1];
//     const baseSql = `SELECT SUM(usfl.tot_email_sent) AS total_email_sent
//     FROM user_stakeholder_form_logs AS usfl
//     WHERE usfl.user_stakeholder_form_id = ? AND usfl.wave = ?
//     /*EXTRA_FILTERS*/
//     GROUP BY usfl.user_stakeholder_form_id, usfl.wave`;
//     //console.log(baseSql);
//     const tableStr = `fdc`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [responsesRows] = await pool.query(sql, [form_id, wave_id]);
//     console.log(responsesRows);
//     const formsSentRows = responsesRows?.[0]?.total_email_sent || 0;
//     return { formsSentRows };
//   } else if (suffix === "form_segment") {
//     const form_id = params[0];
//     const segment_id = params[1];
//     const baseSql = `WITH
// -- Split client_users into rows
// c_ids AS (
//     SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(c.client_users, ',', n.n), ',', -1) AS id
//     FROM client_segment_users c
//     JOIN (
//         SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
//         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
//         UNION ALL SELECT 9 UNION ALL SELECT 10
//     ) n
//     ON n.n <= 1 + LENGTH(c.client_users) - LENGTH(REPLACE(c.client_users, ',', ''))
//     WHERE c.segment_id = ?
// ),

// -- Split total_email_sent into rows
// l_ids AS (
//     SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(g.total_email_sent, ',', m.m), ',', -1) AS id
//     FROM (
//         SELECT GROUP_CONCAT(DISTINCT sent_ids ORDER BY sent_ids) AS total_email_sent
//         FROM user_stakeholder_form_logs
//         WHERE user_stakeholder_form_id = ?
//     ) g
//     JOIN (
//         SELECT 1 m UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
//         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
//         UNION ALL SELECT 9 UNION ALL SELECT 10
//     ) m
//     ON m.m <= 1 + LENGTH(g.total_email_sent) - LENGTH(REPLACE(g.total_email_sent, ',', ''))
// )

// -- Final intersection + aggregation
// SELECT COUNT(DISTINCT c_ids.id) AS total_email_sent
// FROM c_ids
// JOIN l_ids ON c_ids.id = l_ids.id;`;
//     //console.log(baseSql);
//     const tableStr = `fdc`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [responsesRows] = await pool.query(sql, [segment_id, form_id]);
//     console.log(responsesRows);
//     const formsSentRows = responsesRows?.[0]?.total_email_sent || 0;
//     return { formsSentRows };
//   } else if (suffix == "form_wave_segment") {
//     const form_id = params[0];
//     const wave_id = params[1];
//     const segment_id = params[2];
//     const company_id = params[3];
//     const baseSql = `WITH
// -- Split client_users into rows
// c_ids AS (
//     SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(c.client_users, ',', n.n), ',', -1) AS id
//     FROM client_segment_users c
//     JOIN (
//         SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
//         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
//         UNION ALL SELECT 9 UNION ALL SELECT 10
//     ) n
//     ON n.n <= 1 + LENGTH(c.client_users) - LENGTH(REPLACE(c.client_users, ',', ''))
//     WHERE c.segment_id = ?
// ),

// -- Split total_email_sent into rows
// l_ids AS (
//     SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(g.total_email_sent, ',', m.m), ',', -1) AS id
//     FROM (
//         SELECT GROUP_CONCAT(DISTINCT sent_ids ORDER BY sent_ids) AS total_email_sent
//         FROM user_stakeholder_form_logs
//         WHERE user_stakeholder_form_id = ? AND wave = ?
//     ) g
//     JOIN (
//         SELECT 1 m UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
//         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
//         UNION ALL SELECT 9 UNION ALL SELECT 10
//     ) m
//     ON m.m <= 1 + LENGTH(g.total_email_sent) - LENGTH(REPLACE(g.total_email_sent, ',', ''))
// )

// -- Final intersection + aggregation
// SELECT COUNT(DISTINCT c_ids.id) AS total_email_sent
// FROM c_ids
// JOIN l_ids ON c_ids.id = l_ids.id;`;
//     //console.log(baseSql);
//     const tableStr = `fdc`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [responsesRows] = await pool.query(sql, [
//       segment_id,
//       form_id,
//       wave_id,
//     ]);
//     const formsSentRows = responsesRows?.[0]?.total_email_sent || 0;
//     return { formsSentRows };
//   }
// }

// export async function getWaveCategoryPercentages(formId, company) {
//   // Validate the dynamic suffix is a simple integer
//   const companySuffix = Number(company);
//   if (!Number.isFinite(companySuffix)) {
//     throw new Error("Invalid company id for dynamic table");
//   }

//   const sql = `
//     WITH AvgScores AS (
//       SELECT
//         wave_id,
//         respondent_id,
//         ROUND(AVG(option_numeric), 2) AS avg_score
//       FROM \`form_data_company${companySuffix}\`
//       WHERE form_id = ?
//         AND option_numeric REGEXP '^[0-9]+$'
//       GROUP BY wave_id, respondent_id
//     ),
//     ClassifiedResponses AS (
//       SELECT
//         wave_id,
//         respondent_id,
//         avg_score,
//         CASE
//           WHEN avg_score >= 4 THEN 'Green'
//           WHEN avg_score >= 3 THEN 'Amber'
//           ELSE 'Red'
//         END AS response_category
//       FROM AvgScores
//     )
//     SELECT
//       wave_id,
//       ROUND(COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
//       ROUND(COUNT(CASE WHEN response_category = 'Red'   THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
//       ROUND(COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage
//     FROM ClassifiedResponses
//     GROUP BY wave_id
//     ORDER BY wave_id DESC
//   `;
//   const [rows] = await pool.query(sql, [formId]);
//   return rows;
// }

//question view drilldown
/** 2) getCriticalIssues */
// export async function part2GetCriticalIssues(filters) {
//   const { suffix, params } = pickVariant(filters);
//   //
//   if (suffix == "form") {
//     const form_id = params[0];
//     const company_id = params[1];
//     const baseSql = `
//                 WITH all_waves AS (
//                     SELECT DISTINCT fdc.wave_id
//                     FROM form_data_company${company_id} fdc
//                     WHERE fdc.form_id = ?
//                 )
//                 , below_thresholds AS (
//                     SELECT
//                         fdc.wave_id,
//                         fdc.question_id,
//                         ROUND(AVG(fdc.option_numeric), 2) AS avg_score
//                     FROM form_data_company${company_id} AS fdc
//                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                     WHERE fdc.form_id = ?
//                       AND fdc.option_numeric REGEXP '^[0-9]+$'
//                       /*EXTRA_FILTERS*/
//                     GROUP BY fdc.wave_id, fdc.question_id
//                 )
//                 , counts AS (
//                     SELECT
//                         t.wave_id,
//                         COUNT(*) AS below_threshold_count
//                     FROM below_thresholds t
//                     WHERE t.avg_score < (
//                         SELECT COALESCE(u.critical_threshold, 2.5)
//                         FROM user u
//                         WHERE u.id = ?
//                     )
//                     GROUP BY t.wave_id
//                 )
//                 SELECT
//                     aw.wave_id,
//                     COALESCE(c.below_threshold_count, 0) AS below_threshold_count
//                 FROM all_waves aw
//                 LEFT JOIN counts c ON c.wave_id = aw.wave_id
//                 ORDER BY aw.wave_id DESC`;
//     //console.log(baseSql);
//     const tableStr = `cu`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     const [rows] = await pool.query(sql, [form_id, form_id, company_id]);
//     //console.log(rows);
//     return rows;
//   } else if (suffix == "form_wave") {
//     const form_id = params[0];
//     const company_id = params[2];
//     const baseSql = `
//                 WITH all_waves AS (
//                     SELECT DISTINCT fdc.wave_id
//                     FROM form_data_company${company_id} fdc
//                     WHERE fdc.form_id = ?
//                 )
//                 , below_thresholds AS (
//                     SELECT
//                         fdc.wave_id,
//                         fdc.question_id,
//                         ROUND(AVG(fdc.option_numeric), 2) AS avg_score
//                     FROM form_data_company${company_id} AS fdc
//                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                     WHERE fdc.form_id = ?
//                       AND fdc.option_numeric REGEXP '^[0-9]+$'
//                       /*EXTRA_FILTERS*/
//                     GROUP BY fdc.wave_id, fdc.question_id
//                 )
//                 , counts AS (
//                     SELECT
//                         t.wave_id,
//                         COUNT(*) AS below_threshold_count
//                     FROM below_thresholds t
//                     WHERE t.avg_score < (
//                         SELECT COALESCE(u.critical_threshold, 2.5)
//                         FROM user u
//                         WHERE u.id = ?
//                     )
//                     GROUP BY t.wave_id
//                 )
//                 SELECT
//                     aw.wave_id,
//                     COALESCE(c.below_threshold_count, 0) AS below_threshold_count
//                 FROM all_waves aw
//                 LEFT JOIN counts c ON c.wave_id = aw.wave_id
//                 ORDER BY aw.wave_id DESC`;
//     //console.log(baseSql);
//     const tableStr = `cu`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     const [rows] = await pool.query(sql, [form_id, form_id, company_id]);
//     //console.log(rows);
//     return rows;
//   } else if (suffix === "form_segment") {
//     const form_id = params[0];
//     const segment_id = params[1];
//     const company_id = params[2];
//     const baseSql = `
//                 WITH all_waves AS (
//                     SELECT DISTINCT fdc.wave_id
//                     FROM form_data_company${company_id} fdc
//                     WHERE fdc.form_id = ?
//                 )
//                 , below_thresholds AS (
//                     SELECT
//                         fdc.wave_id,
//                         fdc.question_id,
//                         ROUND(AVG(fdc.option_numeric), 2) AS avg_score
//                     FROM form_data_company${company_id} AS fdc
//                     INNER JOIN client_segment_users csu
//                         ON csu.segment_id = ?
//                       AND csu.company = ?
//                       AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                     WHERE fdc.form_id = ?
//                       AND fdc.option_numeric REGEXP '^[0-9]+$'
//                       /*EXTRA_FILTERS*/
//                     GROUP BY fdc.wave_id, fdc.question_id
//                 )
//                 , counts AS (
//                     SELECT
//                         t.wave_id,
//                         COUNT(*) AS below_threshold_count
//                     FROM below_thresholds t
//                     WHERE t.avg_score < (
//                         SELECT COALESCE(u.critical_threshold, 2.5)
//                         FROM user u
//                         WHERE u.id = ?
//                     )
//                     GROUP BY t.wave_id
//                 )
//                 SELECT
//                     aw.wave_id,
//                     COALESCE(c.below_threshold_count, 0) AS below_threshold_count
//                 FROM all_waves aw
//                 LEFT JOIN counts c ON c.wave_id = aw.wave_id
//                 ORDER BY aw.wave_id DESC`;
//     //console.log(baseSql);
//     const tableStr = `cu`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [rows] = await pool.query(sql, [
//       form_id,
//       segment_id,
//       company_id,
//       form_id,
//       company_id,
//     ]);
//     //console.log(rows);
//     return rows;
//   } else if (suffix == "form_wave_segment") {
//     const form_id = params[0];
//     const segment_id = params[2];
//     const company_id = params[3];
//     const baseSql = `
//                 WITH all_waves AS (
//                     SELECT DISTINCT fdc.wave_id
//                     FROM form_data_company${company_id} fdc
//                     WHERE fdc.form_id = ?
//                 )
//                 , below_thresholds AS (
//                     SELECT
//                         fdc.wave_id,
//                         fdc.question_id,
//                         ROUND(AVG(fdc.option_numeric), 2) AS avg_score
//                     FROM form_data_company${company_id} AS fdc
//                     INNER JOIN client_segment_users csu
//                         ON csu.segment_id = ?
//                       AND csu.company = ?
//                       AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                     WHERE fdc.form_id = ?
//                       AND fdc.option_numeric REGEXP '^[0-9]+$'
//                       /*EXTRA_FILTERS*/
//                     GROUP BY fdc.wave_id, fdc.question_id
//                 )
//                 , counts AS (
//                     SELECT
//                         t.wave_id,
//                         COUNT(*) AS below_threshold_count
//                     FROM below_thresholds t
//                     WHERE t.avg_score < (
//                         SELECT COALESCE(u.critical_threshold, 2.5)
//                         FROM user u
//                         WHERE u.id = ?
//                     )
//                     GROUP BY t.wave_id
//                 )
//                 SELECT
//                     aw.wave_id,
//                     COALESCE(c.below_threshold_count, 0) AS below_threshold_count
//                 FROM all_waves aw
//                 LEFT JOIN counts c ON c.wave_id = aw.wave_id
//                 ORDER BY aw.wave_id DESC`;
//     //console.log(baseSql);
//     const tableStr = `cu`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [rows] = await pool.query(sql, [
//       form_id,
//       segment_id,
//       company_id,
//       form_id,
//       company_id,
//     ]);
//     //console.log(rows);
//     return rows;
//   }
// }

//form view
//22222222222222222222222222222222
/** 2) getCriticalIssues */
// export async function part2GetCriticalIssues(filters) {
//   const { suffix, params } = pickVariant(filters);
//   //
//   if (suffix == "form") {
//     const form_id = params[0];
//     const company_id = params[1];
//     const baseSql = `
//                 WITH all_waves AS (
//                     SELECT DISTINCT fdc.wave_id
//                     FROM form_data_company${company_id} fdc
//                     WHERE fdc.form_id = ?
//                 )
//                 , below_thresholds AS (
//                     SELECT
//                         fdc.wave_id,
//                         fdc.question_id,
//                         ROUND(AVG(fdc.option_numeric), 2) AS avg_score
//                     FROM form_data_company${company_id} AS fdc
//                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                     WHERE fdc.form_id = ?
//                       AND fdc.option_numeric REGEXP '^[0-9]+$'
//                       /*EXTRA_FILTERS*/
//                     GROUP BY fdc.wave_id, fdc.question_id
//                 )
//                 , counts AS (
//                     SELECT
//                         t.wave_id,
//                         COUNT(*) AS below_threshold_count
//                     FROM below_thresholds t
//                     WHERE t.avg_score < (
//                         SELECT COALESCE(u.critical_threshold, 2.5)
//                         FROM user u
//                         WHERE u.id = ?
//                     )
//                     GROUP BY t.wave_id
//                 )
//                 SELECT
//                     aw.wave_id,
//                     COALESCE(c.below_threshold_count, 0) AS below_threshold_count
//                 FROM all_waves aw
//                 LEFT JOIN counts c ON c.wave_id = aw.wave_id
//                 ORDER BY aw.wave_id DESC`;
//     //console.log(baseSql);
//     const tableStr = `cu`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     const [rows] = await pool.query(sql, [form_id, form_id, company_id]);
//     //console.log(rows);
//     return rows;
//   } else if (suffix == "form_wave") {
//     const form_id = params[0];
//     const company_id = params[2];
//     const baseSql = `
//                 WITH all_waves AS (
//                     SELECT DISTINCT fdc.wave_id
//                     FROM form_data_company${company_id} fdc
//                     WHERE fdc.form_id = ?
//                 )
//                 , below_thresholds AS (
//                     SELECT
//                         fdc.wave_id,
//                         fdc.question_id,
//                         ROUND(AVG(fdc.option_numeric), 2) AS avg_score
//                     FROM form_data_company${company_id} AS fdc
//                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                     WHERE fdc.form_id = ?
//                       AND fdc.option_numeric REGEXP '^[0-9]+$'
//                       /*EXTRA_FILTERS*/
//                     GROUP BY fdc.wave_id, fdc.question_id
//                 )
//                 , counts AS (
//                     SELECT
//                         t.wave_id,
//                         COUNT(*) AS below_threshold_count
//                     FROM below_thresholds t
//                     WHERE t.avg_score < (
//                         SELECT COALESCE(u.critical_threshold, 2.5)
//                         FROM user u
//                         WHERE u.id = ?
//                     )
//                     GROUP BY t.wave_id
//                 )
//                 SELECT
//                     aw.wave_id,
//                     COALESCE(c.below_threshold_count, 0) AS below_threshold_count
//                 FROM all_waves aw
//                 LEFT JOIN counts c ON c.wave_id = aw.wave_id
//                 ORDER BY aw.wave_id DESC`;
//     //console.log(baseSql);
//     const tableStr = `cu`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     const [rows] = await pool.query(sql, [form_id, form_id, company_id]);
//     //console.log(rows);
//     return rows;
//   } else if (suffix === "form_segment") {
//     const form_id = params[0];
//     const segment_id = params[1];
//     const company_id = params[2];
//     const baseSql = `
//                 WITH all_waves AS (
//                     SELECT DISTINCT fdc.wave_id
//                     FROM form_data_company${company_id} fdc
//                     WHERE fdc.form_id = ?
//                 )
//                 , below_thresholds AS (
//                     SELECT
//                         fdc.wave_id,
//                         fdc.question_id,
//                         ROUND(AVG(fdc.option_numeric), 2) AS avg_score
//                     FROM form_data_company${company_id} AS fdc
//                     INNER JOIN client_segment_users csu
//                         ON csu.segment_id = ?
//                       AND csu.company = ?
//                       AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                     WHERE fdc.form_id = ?
//                       AND fdc.option_numeric REGEXP '^[0-9]+$'
//                       /*EXTRA_FILTERS*/
//                     GROUP BY fdc.wave_id, fdc.question_id
//                 )
//                 , counts AS (
//                     SELECT
//                         t.wave_id,
//                         COUNT(*) AS below_threshold_count
//                     FROM below_thresholds t
//                     WHERE t.avg_score < (
//                         SELECT COALESCE(u.critical_threshold, 2.5)
//                         FROM user u
//                         WHERE u.id = ?
//                     )
//                     GROUP BY t.wave_id
//                 )
//                 SELECT
//                     aw.wave_id,
//                     COALESCE(c.below_threshold_count, 0) AS below_threshold_count
//                 FROM all_waves aw
//                 LEFT JOIN counts c ON c.wave_id = aw.wave_id
//                 ORDER BY aw.wave_id DESC`;
//     //console.log(baseSql);
//     const tableStr = `cu`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [rows] = await pool.query(sql, [
//       form_id,
//       segment_id,
//       company_id,
//       form_id,
//       company_id,
//     ]);
//     //console.log(rows);
//     return rows;
//   } else if (suffix == "form_wave_segment") {
//     const form_id = params[0];
//     const segment_id = params[2];
//     const company_id = params[3];
//     const baseSql = `
//                 WITH all_waves AS (
//                     SELECT DISTINCT fdc.wave_id
//                     FROM form_data_company${company_id} fdc
//                     WHERE fdc.form_id = ?
//                 )
//                 , below_thresholds AS (
//                     SELECT
//                         fdc.wave_id,
//                         fdc.question_id,
//                         ROUND(AVG(fdc.option_numeric), 2) AS avg_score
//                     FROM form_data_company${company_id} AS fdc
//                     INNER JOIN client_segment_users csu
//                         ON csu.segment_id = ?
//                       AND csu.company = ?
//                       AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
//                     INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
//                     WHERE fdc.form_id = ?
//                       AND fdc.option_numeric REGEXP '^[0-9]+$'
//                       /*EXTRA_FILTERS*/
//                     GROUP BY fdc.wave_id, fdc.question_id
//                 )
//                 , counts AS (
//                     SELECT
//                         t.wave_id,
//                         COUNT(*) AS below_threshold_count
//                     FROM below_thresholds t
//                     WHERE t.avg_score < (
//                         SELECT COALESCE(u.critical_threshold, 2.5)
//                         FROM user u
//                         WHERE u.id = ?
//                     )
//                     GROUP BY t.wave_id
//                 )
//                 SELECT
//                     aw.wave_id,
//                     COALESCE(c.below_threshold_count, 0) AS below_threshold_count
//                 FROM all_waves aw
//                 LEFT JOIN counts c ON c.wave_id = aw.wave_id
//                 ORDER BY aw.wave_id DESC`;
//     //console.log(baseSql);
//     const tableStr = `cu`;
//     const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
//     //console.log(sql);
//     const [rows] = await pool.query(sql, [
//       form_id,
//       segment_id,
//       company_id,
//       form_id,
//       company_id,
//     ]);
//     //console.log(rows);
//     return rows;
//   }
// }

/* 
      <!-- Preheader -->
      <%#
      <p style="font-weight: bold; font-size: 14px; margin-top: 0">
        %> <%# <%= preheader.replace('{{Donation Amount}}', donation_amount) %>
        %> <%#
      </p>
      %> */
/* 

            <!-- Body Text -->
      <!-- <p style="margin-top: 10px; font-size: 14px; line-height: 1.6"> -->
      <%= body.replace('{{ respondent_name }}', name) %>
      <!-- </p> --> */
/** 2) getRespondentLatestLocation */
export async function getRespondentLatestLocation(clientUserId, company_id) {
  const sql = `
    SELECT 
    fdc.respondent_id, 
    cu.city, 
    cu.region, 
    cu.country,
    TRIM(
        BOTH ' ,-' FROM CONCAT(
          IFNULL(cu.city, ''),
          IF(cu.region IS NOT NULL AND cu.region != '', CONCAT(', ', cu.region), ''),
          IF(cu.country IS NOT NULL AND cu.country != '', CONCAT(' - ', cu.country), '')
        )
      ) AS location
    FROM form_data_company${company_id} fdc
    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
    WHERE fdc.respondent_id = ? AND cu.city IS NOT NULL AND cu.city != '' AND cu.region IS NOT NULL AND cu.region != '' AND cu.country IS NOT NULL AND cu.country != ''
    ORDER BY fdc.surrogate_key DESC
    LIMIT 1
  `;
  const [rows] = await pool.query(sql, [clientUserId]);
  //console.log(rows);
  return rows[0] || {};
}

/** 2) getRespondentLatestLocation */
export async function getRespondentLatestLocation1(
  clientUserId,
  company_id,
  stakeholder_id
) {
  const sql = `
    SELECT 
    fdc.respondent_id,
    cu.city,
    cu.region,
    cu.country
    FROM form_data_company${company_id} fdc
    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
    WHERE fdc.respondent_id = ? AND cu.city IS NOT NULL AND cu.city != '' AND cu.region IS NOT NULL AND cu.region != '' AND cu.country IS NOT NULL AND cu.country != ''
    ORDER BY fdc.surrogate_key DESC
    LIMIT 1
  `;
  const [rows] = await pool.query(sql, [stakeholder_id, clientUserId]);
  //console.log(rows);
  return rows[0] || {};
}
