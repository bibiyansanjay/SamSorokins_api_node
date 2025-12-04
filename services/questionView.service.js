// services/formView.service.js
import pool from "../db";

//((((((((((((((((((((((((((((((((((((((helpers starts))))))))))))))))))))))))))))))))))))))
function pickVariant({
  form_id,
  question_id,
  wave_id,
  segment_id,
  company_id,
  city,
  region,
  country,
}) {
  if (company_id && form_id && question_id && !wave_id && !segment_id)
    return {
      suffix: "form",
      params: [form_id, question_id, company_id, city, region, country],
    };
  if (company_id && form_id && question_id && wave_id && !segment_id)
    return {
      suffix: "form_wave",
      params: [
        form_id,
        question_id,
        wave_id,
        company_id,
        city,
        region,
        country,
      ],
    };
  if (company_id && form_id && question_id && !wave_id && segment_id)
    return {
      suffix: "form_segment",
      params: [
        form_id,
        question_id,
        segment_id,
        company_id,
        city,
        region,
        country,
      ],
    };
  if (company_id && form_id && question_id && wave_id && segment_id)
    return {
      suffix: "form_wave_segment",
      params: [
        form_id,
        question_id,
        wave_id,
        segment_id,
        company_id,
        city,
        region,
        country,
      ],
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
//

async function getQuestionMeta(question_id) {
  // Assumes 'question' table links to 'question_type'
  const sql = `
    SELECT
      q.id AS question_id,
      q.question_type as question_type_id,
      qt.name AS question_type_name,
      q.question_text_en,
      q.question_text_nl,
      -- qt.response_options_en,
      -- q.answer_options_en  AS custom_options_en
                  CASE
                    WHEN qt.name = 'Single-Select (5 options)'
                      THEN q.answer_options_en
                    ELSE qt.response_options_en
                  END AS answer_option
    FROM question q
    LEFT JOIN question_type qt ON qt.id = q.question_type
    WHERE q.id = ?
    LIMIT 1
  `;
  const [rows] = await pool.query(sql, [question_id]);
  //console.log("question data", rows);
  return rows?.[0] || null;
}

async function getHighestOption(rows) {
  if (!rows || rows.length === 0) return null;

  let highest = rows[0];
  for (const row of rows) {
    const currentPct = parseFloat(row.pct_of_total);
    const highestPct = parseFloat(highest.pct_of_total);

    if (currentPct > highestPct) {
      highest = row;
    } else if (currentPct === highestPct) {
      // tie → prefer the later one
      highest = row;
    }
  }
  return highest;
}
//((((((((((((((((((((((((((((((((((((((helpers ends))))))))))))))))))))))))))))))))))))))
//
export async function getQuestionData(question_id) {
  // Assumes 'question' table links to 'question_type'
  const sql = `
    SELECT 
    q.id,
    q.question_type,
    q.question_text_en,
    q.question_text_nl,
    q.answer_options_en,
    q.answer_options_nl,
    q.three_word_outcome_en,
    q.three_word_outcome_nl,
    q.acronyms_en,
    q.acronyms_nl,
    q.stakeholder_id,
    q.subcategory_id,
    q.category_id,
    q.kpi_id,
    q.polarity,
    q.priority,
    q.reinforcement_q_id1,
    q.reinforcement_type1,
    q.reinforcement_q_id2,
    q.reinforcement_type2,
    CONCAT_WS('-', NULLIF(scat.abbreviation, ''), NULLIF(sth.abbreviation, ''), q.id) AS QID,
    q.isActive,
    q.isDeleted,
    q.deleted_at,
    q.created_at,
    q.updated_at,
    q.created_by,
    q.company,
    CONCAT(cb.firstName, ' ', cb.lastName) AS created_by_name,
    CONCAT(cmp.firstName, ' ', cmp.lastName) AS company_name,
    sth.name AS stakeholder_name,
    scat.name AS subcategory_name,
    cat.name AS category_name,
    kpi.name AS kpi_name,
    qty.name AS question_type_name
    FROM \`question\` q
    LEFT JOIN \`user\` cb ON q.created_by = cb.id
    LEFT JOIN \`user\` cmp ON q.company = cmp.id
    LEFT JOIN \`stakeholder\` sth ON q.stakeholder_id = sth.id
    LEFT JOIN \`subcategory\` scat ON q.subcategory_id = scat.id
    LEFT JOIN \`category\` cat ON q.category_id = cat.id
    LEFT JOIN \`kpi\` kpi ON q.kpi_id = kpi.id
    LEFT JOIN \`question_type\` qty ON q.question_type = qty.id
    WHERE q.id = ?
  `;
  const [rows] = await pool.query(sql, [question_id]);
  //console.log("question data", rows);
  return rows?.[0] || null;
}
/** 1) Rag Score */
export async function getRagScore(filters) {
  const { suffix, params } = pickVariant(filters);
  const form_id = params[0];
  const question_id = params[1];
  //
  if (suffix == "form") {
    const company_id = params[2];
    const baseSql = `
              SELECT
                    ROUND(AVG(fdc.option_numeric), 2) AS rag_score
                  FROM form_data_company${company_id} AS fdc
                  INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                  WHERE fdc.form_id = ?  
                    AND fdc.question_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
            `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [form_id, question_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "form_wave") {
    const wave_id = params[2];
    const company_id = params[3];
    const baseSql = `
              SELECT
                    ROUND(AVG(fdc.option_numeric), 2) AS rag_score
                  FROM form_data_company${company_id} AS fdc
                  INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                  WHERE fdc.form_id = ? 
                    AND fdc.question_id = ? 
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    AND fdc.wave_id = ?
                    /*EXTRA_FILTERS*/
            `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [form_id, question_id, wave_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "form_segment") {
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `
              SELECT 
                    ROUND(AVG(fdc.option_numeric), 2) AS rag_score
                  FROM form_data_company${company_id} AS fdc
                  INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                  INNER JOIN client_segment_users AS csu  
                    ON csu.segment_id = ? AND csu.company=?
                    AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                  WHERE fdc.form_id = ?  
                    AND fdc.question_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
            `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      form_id,
      question_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "form_wave_segment") {
    const wave_id = params[2];
    const segment_id = params[3];
    const company_id = params[4];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        WHERE fdc.form_id = ? 
          AND fdc.question_id = ? 
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      form_id,
      question_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 2) Trend (avg delta for latest/selected wave) */
export async function getTrend(filters) {
  const { suffix, params } = pickVariant(filters);
  const form_id = params[0];
  const question_id = params[1];
  //
  if (suffix === "form") {
    const company_id = params[2];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 24
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                      AND question_id = ?
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
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [form_id, question_id]);
    return rows;
  } else if (suffix === "form_wave") {
    const wave_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH base AS (
                    SELECT
                      fdc.form_id,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric), 2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    WHERE fdc.form_id = ?              -- e.g. 24
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                      AND question_id = ?
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
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [form_id, question_id, wave_id]);
    return rows;
  } else if (suffix === "form_segment") {
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `WITH base AS (
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
                    WHERE fdc.form_id = ?              -- e.g. 24
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                      AND question_id = ?
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
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      form_id,
      question_id,
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "form_wave_segment") {
    const wave_id = params[2];
    const segment_id = params[3];
    const company_id = params[4];
    const baseSql = `WITH base AS (
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
                    WHERE fdc.form_id = ?              -- e.g. 24
                      AND fdc.option_numeric REGEXP '^[0-9]+$'
                      AND question_id = ?
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
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      form_id,
      question_id,
      wave_id,
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 3) options and their percentages/highest options as well */
export async function getOptionsPerc(filters) {
  const { suffix, params } = pickVariant(filters);
  const form_id = params[0];
  const question_id = params[1];
  //
  const meta = await getQuestionMeta(question_id);
  if (!meta) throw new Error("Question not found");
  const options = meta.answer_option.split("|").map((opt) => opt.trim());
  //console.log("Parsed options:", options);
  const optionsCTE = options
    .map(
      (opt, i) => `SELECT ${pool.escape(opt)} AS answer, ${i + 1} AS sort_order`
    )
    .join(" UNION ALL ");
  //console.log("options", options);
  //
  if (suffix == "form") {
    const company_id = params[2];
    const baseSql = `
                WITH options AS (
                  ${optionsCTE}
                ),
                counts AS (
                  SELECT
                    fdc.form_id,
                    fdc.question_id,
                    fdc.answer,
                    COUNT(*) AS occurrences,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
                  FROM form_data_company${company_id} AS fdc
                  INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                  WHERE fdc.form_id = ?
                    AND fdc.question_id = ?
                    /*EXTRA_FILTERS*/
                  GROUP BY fdc.form_id, fdc.question_id, fdc.answer
                )
                SELECT
                  ? AS form_id,
                  ? AS question_id,
                  o.answer,
                  COALESCE(c.occurrences, 0) AS occurrences,
                  COALESCE(c.pct_of_total, 0) AS pct_of_total
                FROM options o
                LEFT JOIN counts c
                  ON o.answer = c.answer
                ORDER BY o.sort_order
              `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    // console.log(sql);
    const [rows] = await pool.query(sql, [
      form_id,
      question_id,
      form_id,
      question_id,
    ]);
    //console.log(rows);
    const highestOption = await getHighestOption(rows);
    //console.log("Highest option:", highestOption);
    return { rows, highestOption };
  } else if (suffix == "form_wave") {
    const wave_id = params[2];
    const company_id = params[3];
    const baseSql = `
                WITH options AS (
                  ${optionsCTE}
                ),
                counts AS (
                  SELECT
                    fdc.form_id,
                    fdc.question_id,
                    fdc.answer,
                    COUNT(*) AS occurrences,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
                  FROM form_data_company${company_id} AS fdc
                  INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                  WHERE fdc.form_id = ?
                    AND fdc.question_id = ?
                    AND fdc.wave_id = ?
                    /*EXTRA_FILTERS*/
                  GROUP BY fdc.form_id, fdc.question_id, fdc.answer
                )
                SELECT
                  ? AS form_id,
                  ? AS question_id,
                  o.answer,
                  COALESCE(c.occurrences, 0) AS occurrences,
                  COALESCE(c.pct_of_total, 0) AS pct_of_total
                FROM options o
                LEFT JOIN counts c
                  ON o.answer = c.answer
                ORDER BY o.sort_order
              `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    // console.log(sql);
    const [rows] = await pool.query(sql, [
      form_id,
      question_id,
      wave_id,
      form_id,
      question_id,
    ]);
    console.log(rows);
    const highestOption = await getHighestOption(rows);
    //console.log("Highest option:", highestOption);
    return { rows, highestOption };
  } else if (suffix == "form_segment") {
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `
                WITH options AS (
                  ${optionsCTE}
                ),
                counts AS (
                  SELECT
                    fdc.form_id,
                    fdc.question_id,
                    fdc.answer,
                    COUNT(*) AS occurrences,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
                  FROM form_data_company${company_id} AS fdc
                  INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                INNER JOIN client_segment_users AS csu
                  ON csu.segment_id = ?            
                  AND csu.company = ?               
                  AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                  
                  WHERE fdc.form_id = ?
                    AND fdc.question_id = ?
                    /*EXTRA_FILTERS*/
                  GROUP BY fdc.form_id, fdc.question_id, fdc.answer
                )
                SELECT
                  ? AS form_id,
                  ? AS question_id,
                  o.answer,
                  COALESCE(c.occurrences, 0) AS occurrences,
                  COALESCE(c.pct_of_total, 0) AS pct_of_total
                FROM options o
                LEFT JOIN counts c
                  ON o.answer = c.answer
                ORDER BY o.sort_order
              `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      form_id,
      question_id,
      form_id,
      question_id,
    ]);
    //console.log(rows);
    const highestOption = await getHighestOption(rows);
    //console.log("Highest option:", highestOption);
    return { rows, highestOption };
  } else if (suffix == "form_wave_segment") {
    const wave_id = params[2];
    const segment_id = params[3];
    const company_id = params[4];
    const baseSql = `
                WITH options AS (
                  ${optionsCTE}
                ),
                counts AS (
                  SELECT
                    fdc.form_id,
                    fdc.question_id,
                    fdc.answer,
                    COUNT(*) AS occurrences,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
                  FROM form_data_company${company_id} AS fdc
                  INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                INNER JOIN client_segment_users AS csu
                  ON csu.segment_id = ?            
                  AND csu.company = ?               
                  AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                  
                  WHERE fdc.form_id = ?
                    AND fdc.question_id = ?
                    AND fdc.wave_id = ?
                    /*EXTRA_FILTERS*/
                  GROUP BY fdc.form_id, fdc.question_id, fdc.answer
                )
                SELECT
                  ? AS form_id,
                  ? AS question_id,
                  o.answer,
                  COALESCE(c.occurrences, 0) AS occurrences,
                  COALESCE(c.pct_of_total, 0) AS pct_of_total
                FROM options o
                LEFT JOIN counts c
                  ON o.answer = c.answer
                ORDER BY o.sort_order
              `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      form_id,
      question_id,
      wave_id,
      form_id,
      question_id,
    ]);
    //console.log(rows);
    const highestOption = await getHighestOption(rows);
    //console.log("Highest option:", highestOption);
    return { rows, highestOption };
  }
}


/** 4) Respondents Data (dynamic table name via company lookup) */
export async function getRespondentsData(filters) {
  const { suffix, params } = pickVariant(filters);
  const form_id = params[0];
  const question_id = params[1];
  //
  if (suffix === "form") {
    const company_id = params[2];
    const baseSql = `
                WITH per_wave AS (
                SELECT 
                    fdc.form_id,
                    fdc.question_id, 
                    fdc.respondent_id, 
                    fdc.wave_id, 
                    cu.city,
                    fdc.created_at,
                    DATE_FORMAT(MAX(fdc.created_at), '%d %M %Y') AS response_date, 
                    ROUND(AVG(fdc.option_numeric), 2) AS avg_score 
                FROM form_data_company${company_id} AS fdc
                INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                WHERE fdc.form_id = ?
                  AND fdc.question_id = ?
                -- AND fdc.option_numeric REGEXP '^[0-9]+$'
                /*EXTRA_FILTERS*/
                GROUP BY 
                    fdc.form_id, 
                    fdc.question_id, 
                    fdc.respondent_id, 
                    fdc.wave_id
            ),
            scored AS (
                SELECT
                    form_id,
                    question_id,
                    respondent_id,
                    wave_id,
                    city,
                    created_at,
                    response_date,
                    ROUND(avg_score, 2) AS rag_score_avg_q,
                    ROUND(
                        avg_score - LAG(avg_score) OVER (
                            PARTITION BY form_id, question_id, respondent_id
                            ORDER BY wave_id
                        ),
                        2
                    ) AS delta_vs_prev_q
                FROM per_wave
            ),
            with_latest AS (
                SELECT
                    s.*,
                    MAX(wave_id) OVER (
                        PARTITION BY form_id, question_id, respondent_id
                    ) AS latest_wave
                FROM scored s
            )
            SELECT
                wl.form_id,
                wl.question_id,
                wl.respondent_id,
                cu.name,
                cu.image,
                ROUND(AVG(wl.rag_score_avg_q), 2) AS rag_score_avg,  -- avg across waves per respondent
                MAX(CASE WHEN wl.wave_id = wl.latest_wave 
                        THEN wl.delta_vs_prev_q END) AS delta_vs_prev,
                MAX(CASE WHEN wl.wave_id = wl.latest_wave
                              THEN wl.city END) AS city,                                       -- city from latest wave
                MAX(CASE WHEN wl.wave_id = wl.latest_wave
                              THEN DATE_FORMAT(wl.created_at, '%d %b %Y') END) AS created_at,   -- created_at from latest wave         
                MAX(wl.latest_wave) AS latest_wave
            FROM with_latest wl
            LEFT JOIN client_users cu 
                ON cu.id = wl.respondent_id
            GROUP BY
                wl.form_id, 
                wl.question_id, 
                wl.respondent_id, 
                cu.name, 
                cu.image
            ORDER BY wl.respondent_id;
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [form_id, question_id]);
    //console.log("Respondents Data:", rows);
    return { result: rows };
  } else if (suffix == "form_wave") {
    const wave_id = params[2];
    const company_id = params[3];
    const baseSql = `
                WITH per_wave AS (
                SELECT 
                    fdc.form_id,
                    fdc.question_id, 
                    fdc.respondent_id, 
                    fdc.wave_id, 
                    cu.city,
                    fdc.created_at,
                    DATE_FORMAT(MAX(fdc.created_at), '%d %M %Y') AS response_date, 
                    ROUND(AVG(fdc.option_numeric), 2) AS avg_score 
                FROM form_data_company${company_id} AS fdc
                INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                WHERE fdc.form_id = ?
                  AND fdc.question_id = ?
                --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                /*EXTRA_FILTERS*/
                GROUP BY 
                    fdc.form_id, 
                    fdc.question_id, 
                    fdc.respondent_id, 
                    fdc.wave_id
            ),
            scored AS (
                SELECT
                    form_id,
                    question_id,
                    respondent_id,
                    wave_id,
                    city,
                    created_at,
                    response_date,
                    ROUND(avg_score, 2) AS rag_score_avg_q,
                    ROUND(
                        avg_score - LAG(avg_score) OVER (
                            PARTITION BY form_id, question_id, respondent_id
                            ORDER BY wave_id
                        ),
                        2
                    ) AS delta_vs_prev_q
                FROM per_wave
            )
            SELECT
                wl.form_id,
                wl.question_id,
                wl.respondent_id,
                wl.city,
                DATE_FORMAT(wl.created_at, '%d %b %Y') AS created_at,
                cu.name,
                cu.image,
                wl.rag_score_avg_q AS rag_score_avg,  -- avg across waves per respondent
                wl.delta_vs_prev_q AS delta_vs_prev
            FROM scored wl
            LEFT JOIN client_users cu 
                ON cu.id = wl.respondent_id
                WHERE wl.wave_id = ?
            GROUP BY
                wl.form_id, 
                wl.question_id, 
                wl.respondent_id, 
                cu.name, 
                cu.image
            ORDER BY wl.respondent_id;
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [form_id, question_id, wave_id]);
    return { result: rows };
  } else if (suffix === "form_segment") {
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `
                WITH per_wave AS (
                SELECT 
                    fdc.form_id,
                    fdc.question_id, 
                    fdc.respondent_id, 
                    fdc.wave_id, 
                    cu.city,
                    fdc.created_at,
                    DATE_FORMAT(MAX(fdc.created_at), '%d %M %Y') AS response_date, 
                    ROUND(AVG(fdc.option_numeric), 2) AS avg_score 
                FROM form_data_company${company_id} AS fdc
                INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                INNER JOIN client_segment_users csu
                    ON csu.company   = ?
                    AND csu.segment_id = ?
                    AND FIND_IN_SET(
                        fdc.respondent_id,
                        REPLACE(COALESCE(csu.client_users, ''), ' ', '')  -- strip spaces like "1, 2,3"
                      ) > 0
                WHERE fdc.form_id = ?
                  AND fdc.question_id = ?
                --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                /*EXTRA_FILTERS*/
                GROUP BY 
                    fdc.form_id, 
                    fdc.question_id, 
                    fdc.respondent_id, 
                    fdc.wave_id
            ),
            scored AS (
                SELECT
                    form_id,
                    question_id,
                    respondent_id,
                    wave_id,
                    city,
                    created_at,
                    response_date,
                    ROUND(avg_score, 2) AS rag_score_avg_q,
                    ROUND(
                        avg_score - LAG(avg_score) OVER (
                            PARTITION BY form_id, question_id, respondent_id
                            ORDER BY wave_id
                        ),
                        2
                    ) AS delta_vs_prev_q
                FROM per_wave
            ),
            with_latest AS (
                SELECT
                    s.*,
                    MAX(wave_id) OVER (
                        PARTITION BY form_id, question_id, respondent_id
                    ) AS latest_wave
                FROM scored s
            )
            SELECT
                wl.form_id,
                wl.question_id,
                wl.respondent_id,
                cu.name,
                cu.image,
                ROUND(AVG(wl.rag_score_avg_q), 2) AS rag_score_avg,  -- avg across waves per respondent
                MAX(CASE WHEN wl.wave_id = wl.latest_wave 
                        THEN wl.delta_vs_prev_q END) AS delta_vs_prev,
                MAX(CASE WHEN wl.wave_id = wl.latest_wave
                              THEN wl.city END) AS city,                                       -- city from latest wave
                MAX(CASE WHEN wl.wave_id = wl.latest_wave
                              THEN DATE_FORMAT(wl.created_at, '%d %b %Y') END) AS created_at,   -- created_at from latest wave        
                MAX(wl.latest_wave) AS latest_wave
            FROM with_latest wl
            LEFT JOIN client_users cu 
                ON cu.id = wl.respondent_id
            GROUP BY
                wl.form_id, 
                wl.question_id, 
                wl.respondent_id, 
                cu.name, 
                cu.image
            ORDER BY wl.respondent_id
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      company_id,
      segment_id,
      form_id,
      question_id,
    ]);
    //console.log(rows);
    return { result: rows };
  } else if (suffix == "form_wave_segment") {
    const wave_id = params[2];
    const segment_id = params[3];
    const company_id = params[4];
    const baseSql = `
                WITH per_wave AS (
                SELECT 
                    fdc.form_id,
                    fdc.question_id, 
                    fdc.respondent_id, 
                    fdc.wave_id, 
                    cu.city,
                    fdc.created_at,
                    DATE_FORMAT(MAX(fdc.created_at), '%d %M %Y') AS response_date, 
                    ROUND(AVG(fdc.option_numeric), 2) AS avg_score 
                FROM form_data_company${company_id} AS fdc
                INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                INNER JOIN client_segment_users csu
                    ON csu.company   = ?
                    AND csu.segment_id = ?
                    AND FIND_IN_SET(
                        fdc.respondent_id,
                        REPLACE(COALESCE(csu.client_users, ''), ' ', '')  -- strip spaces like "1, 2,3"
                      ) > 0
                WHERE fdc.form_id = ?
                  AND fdc.question_id = ?
                --  AND fdc.option_numeric REGEXP '^[0-9]+$'
                /*EXTRA_FILTERS*/
                GROUP BY 
                    fdc.form_id, 
                    fdc.question_id, 
                    fdc.respondent_id, 
                    fdc.wave_id
            ),
            scored AS (
                SELECT
                    form_id,
                    question_id,
                    respondent_id,
                    wave_id,
                    city,
                    created_at,
                    response_date,
                    ROUND(avg_score, 2) AS rag_score_avg_q,
                    ROUND(
                        avg_score - LAG(avg_score) OVER (
                            PARTITION BY form_id, question_id, respondent_id
                            ORDER BY wave_id
                        ),
                        2
                    ) AS delta_vs_prev_q
                FROM per_wave
            )
                 SELECT
                wl.form_id,
                wl.question_id,
                wl.respondent_id,
                wl.city,
                DATE_FORMAT(wl.created_at, '%d %b %Y') AS created_at,
                cu.name,
                cu.image,
                wl.rag_score_avg_q AS rag_score_avg,  -- avg across waves per respondent
                wl.delta_vs_prev_q AS delta_vs_prev
            FROM scored wl
            LEFT JOIN client_users cu 
                ON cu.id = wl.respondent_id
                WHERE wl.wave_id = ?
            GROUP BY
                wl.form_id, 
                wl.question_id, 
                wl.respondent_id, 
                cu.name, 
                cu.image
            ORDER BY wl.respondent_id;
                `;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      company_id,
      segment_id,
      form_id,
      question_id,
      wave_id,
    ]);
    //console.log(rows);
    return { result: rows };
  }
}

/** 5) Total Responses acc to filters */
export async function getTotalResponsesFilters(filters) {
  const { suffix, params } = pickVariant(filters);
  const form_id = params[0];
  //
  if (suffix === "form") {
    const company_id = params[2];
    const baseSql = `SELECT
            fdc.form_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
           WHERE fdc.form_id = ?
           /*EXTRA_FILTERS*/`;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [responsesRows] = await pool.query(sql, [form_id]);
    const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponsesFilters };
  } else if (suffix == "form_wave") {
    const wave_id = params[2];
    const company_id = params[3];
    const baseSql = `SELECT
            fdc.form_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS total_users_sum
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
          WHERE fdc.form_id = ? AND fdc.wave_id=?
          /*EXTRA_FILTERS*/
          GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC`;
    //console.log(baseSql);
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [responsesRows] = await pool.query(sql, [form_id, wave_id]);
    const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponsesFilters };
  } else if (suffix === "form_segment") {
    const segment_id = params[2];
    const company_id = params[3];
    const baseSql = `SELECT
            fdc.form_id,
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
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [responsesRows] = await pool.query(sql, [
      segment_id,
      company_id,
      form_id,
    ]);
    const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponsesFilters };
  } else if (suffix == "form_wave_segment") {
    const wave_id = params[2];
    const segment_id = params[3];
    const company_id = params[4];
    const baseSql = `SELECT
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
    const { sql } = applyExtraFilters(baseSql, filters);
    //console.log(sql);
    const [responsesRows] = await pool.query(sql, [
      segment_id,
      company_id,
      form_id,
      wave_id,
    ]);
    const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponsesFilters };
  }
}


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
    const wave_id = params[2];
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
    const segment_id = params[2];
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
    const wave_id = params[2];
    const segment_id = params[3];
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

/** 6) Get date range according to form n filters */
export async function getDateRange(filters) {
  const { suffix, params } = pickVariant(filters);
  const form_id = params[0];
  //
  if (suffix === "form") {
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
                      LEFT JOIN user_stakeholder_form_logs usfl
                        ON usfl.user_stakeholder_form_id = fdc.form_id
                      AND usfl.wave = fdc.wave_id
                      WHERE fdc.form_id = ?  
                        AND fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [form_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "form_segment") {
    const company_id = params[3];
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
                      WHERE fdc.form_id = ?  
                        AND fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [form_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "form_wave") {
    const wave_id = params[2];
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
    const [rows] = await pool.query(sql, [form_id, wave_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "form_wave_segment") {
    const wave_id = params[2];
    const company_id = params[4];
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
    const [rows] = await pool.query(sql, [form_id, wave_id]);
    //console.log(rows);
    return rows;
  }
}
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/** 1) getAllSegments */
export async function getAllSegments(stakeholderFormId) {
  const sql = `SELECT 
                      s.name as label,
                      s.id as value
                  FROM user_stakeholder_form usf
                  LEFT JOIN segment s 
                      ON FIND_IN_SET(s.id, usf.segments) > 0
                  WHERE usf.id = ?
                  ORDER BY s.name ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholderFormId]);
  return rows;
}

/** 2) getAllWaves */
export async function getAllWaves(stakeholderFormId) {
  const sql = `SELECT DISTINCT CONCAT("Wave ",wave_id)as label, wave_id as value FROM \`wave_question_avg\` where form_id = ? ORDER BY wave_id ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholderFormId]);
  return rows;
}

/** 3) getQuestionType */
export async function getQuestionType(question_id) {
  // Assumes 'question' table links to 'question_type'
  const sql = `
    SELECT
      q.id AS question_id,
      q.question_type as question_type_id,
      qt.name AS question_type_name,
      q.question_text_en,
      q.question_text_nl,
      qt.response_options_en,
      q.answer_options_en  AS custom_options_en
    FROM question q
    LEFT JOIN question_type qt ON qt.id = q.question_type
    WHERE q.id = ?
    LIMIT 1
  `;
  const [rows] = await pool.query(sql, [question_id]);
  //console.log("question data", rows);
  return rows?.[0] || null;
}

/** 4) getCompany */
export async function getCompany(form_id) {
  const sql = `
    SELECT company FROM user_stakeholder_form WHERE id = ?
  `;
  const [companyRows] = await pool.query(sql, [form_id]);
  //console.log("question data", rows);
  const company = companyRows?.[0]?.company;
  //if (!company) return { result: null };
  return company || null;
}

/** 5) Subcategory + Stakeholder data */
export async function getSubcategoryAndStakeholderData(stakeholderFormId) {
  const sql = `
    SELECT sub.id as subcategory_id, CONCAT(cat.abbreviation, '-', LPAD(sub.id, 3, '0')) AS subcategory_formatted_code, sub.name as subcategory_name, sub.image as subcategory_image, sub.image_name as subcategory_image_name, 
           stk.id as stakeholder_id, stk.name as stakeholder_name, stk.image as stakeholder_image, stk.image_name as stakeholder_image_name
    FROM wave_question_avg wqa 
    LEFT JOIN subcategory sub ON wqa.subcategory_id=sub.id 
    LEFT JOIN stakeholder stk ON wqa.stakeholder_id=stk.id 
    LEFT JOIN category cat ON wqa.category_id = cat.id
    WHERE wqa.form_id = ? 
    LIMIT 1
  `;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholderFormId]);
  //console.log(rows);
  return rows;
}

/** 6) Total Responses For the form wdout filters */
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
