// services/formView.service.js
import pool from "../db";

//((((((((((((((((((((((((((((((((((((((helpers starts))))))))))))))))))))))))))))))))))))))
function pickVariant({
  wave_id,
  segment_id,
  company_id,
  city,
  region,
  country,
}) {
  if (!wave_id && !segment_id && company_id)
    return {
      suffix: "stakeholder",
      params: [company_id, city, region, country],
    };
  if (wave_id && !segment_id && company_id)
    return {
      suffix: "stakeholder_wave",
      params: [wave_id, company_id, city, region, country],
    };
  if (!wave_id && segment_id && company_id)
    return {
      suffix: "stakeholder_segment",
      params: [segment_id, company_id, city, region, country],
    };
  if (wave_id && segment_id && company_id)
    return {
      suffix: "stakeholder_wave_segment",
      params: [wave_id, segment_id, company_id, city, region, country],
    };
  throw new Error("Unsupported filter combination");
}

async function applyExtraFilters(baseSql, filters, tableStr) {
  let whereParts = [];
  //console.log("Filters in applyExtraFilters:", filters);

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

/** 1) Rag Score */
export async function getRagScore(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
        WHERE usf.stakeholder_id = ? 
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
  `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
        WHERE usf.stakeholder_id = ?  
          AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const segment_id = params[0];
    const company_id = params[1];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
        WHERE usf.stakeholder_id = ?   
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          /*EXTRA_FILTERS*/
  `;
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
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `
    SELECT 
          ROUND(AVG(fdc.option_numeric), 2) AS rag_score
        FROM form_data_company${company_id} AS fdc
        INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
        INNER JOIN client_segment_users AS csu  
          ON csu.segment_id = ? AND csu.company=?
          AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
        INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
        WHERE usf.stakeholder_id = ? 
          AND fdc.option_numeric REGEXP '^[0-9]+$'
          AND fdc.wave_id = ?
          /*EXTRA_FILTERS*/
  `;
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

/** 2) bestPerformingPack */
export async function bestPerformingPack(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
    const baseSql = `SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY usf.stakeholder_id, usf.subcategory_id
                    ORDER BY avg_score DESC LIMIT 1
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    AND fdc.wave_id = ?
                    /*EXTRA_FILTERS*/
                    GROUP BY usf.stakeholder_id, usf.subcategory_id
                    ORDER BY avg_score DESC LIMIT 1
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, wave_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_segment") {
    const segment_id = params[0];
    const company_id = params[1];
    const baseSql = `SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY usf.stakeholder_id, usf.subcategory_id
                    ORDER BY avg_score DESC LIMIT 1
                `;
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
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    AND fdc.wave_id = ?
                    /*EXTRA_FILTERS*/
                    GROUP BY usf.stakeholder_id, usf.subcategory_id
                    ORDER BY avg_score DESC LIMIT 1
                `;
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

/** 3) worstPerformingPack */
export async function worstPerformingPack(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
    const baseSql = `SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY usf.stakeholder_id, usf.subcategory_id
                    ORDER BY avg_score ASC LIMIT 1
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    AND fdc.wave_id = ?
                    /*EXTRA_FILTERS*/
                    GROUP BY usf.stakeholder_id, usf.subcategory_id
                    ORDER BY avg_score ASC LIMIT 1
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id, wave_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_segment") {
    const segment_id = params[0];
    const company_id = params[1];
    const baseSql = `SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY usf.stakeholder_id, usf.subcategory_id
                    ORDER BY avg_score ASC LIMIT 1
                `;
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
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    AND fdc.wave_id = ?
                    /*EXTRA_FILTERS*/
                    GROUP BY usf.stakeholder_id, usf.subcategory_id
                    ORDER BY avg_score ASC LIMIT 1
                `;
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

/** 4) getFormsCompleted */
export async function getFormsCompleted(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
    const baseSql = `
                    SELECT                  
                    usf.stakeholder_id,
                       COUNT(DISTINCT CONCAT(fdc.form_id, '-', fdc.wave_id)) AS total_submissions
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
            `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `
                    SELECT                  
                      usf.stakeholder_id,
                       fdc.wave_id,
                       COUNT(DISTINCT CONCAT(fdc.form_id, '-', fdc.wave_id)) AS total_submissions
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
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
    const segment_id = params[0];
    const company_id = params[1];
    const baseSql = `
                    SELECT                  
                      usf.stakeholder_id,
                       COUNT(DISTINCT CONCAT(fdc.form_id, '-', fdc.wave_id)) AS total_submissions
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                    
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
            `;
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
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `
                    SELECT                  
                      usf.stakeholder_id,
                       fdc.wave_id,
                       COUNT(DISTINCT CONCAT(fdc.form_id, '-', fdc.wave_id)) AS total_submissions
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
					          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                    
                    WHERE usf.stakeholder_id=?         
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    AND fdc.wave_id = ?
                    /*EXTRA_FILTERS*/
            `;
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

/** 5) Total Responses acc to filters */
export async function getTotalResponsesFilters(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
          SELECT
            usf.stakeholder_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
           WHERE usf.stakeholder_id=?
           /*EXTRA_FILTERS*/
            GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
           )
           SELECT stakeholder_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [responsesRows] = await pool.query(sql, [stakeholder_id]);
    console.log(responsesRows);
    const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponsesFilters };
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
          SELECT
            usf.stakeholder_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
           WHERE usf.stakeholder_id=? AND fdc.wave_id=?
           /*EXTRA_FILTERS*/
            GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
           )
           SELECT stakeholder_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [responsesRows] = await pool.query(sql, [stakeholder_id, wave_id]);
    const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponsesFilters };
  } else if (suffix == "stakeholder_segment") {
    const segment_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
          SELECT
            usf.stakeholder_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
          INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
           WHERE usf.stakeholder_id=?
           /*EXTRA_FILTERS*/
            GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
           )
           SELECT stakeholder_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [responsesRows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
    ]);
    const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponsesFilters };
  } else if (suffix == "stakeholder_wave_segment") {
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
          SELECT
            usf.stakeholder_id,
            fdc.wave_id,
            COUNT(DISTINCT(fdc.respondent_id)) AS sum_wave_level_respondents
          FROM form_data_company${company_id} AS fdc
          INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
          INNER JOIN client_segment_users AS csu
                ON csu.segment_id = ?            
                AND csu.company = ?               
                AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0
           WHERE usf.stakeholder_id=? AND fdc.wave_id=?
           /*EXTRA_FILTERS*/
            GROUP BY fdc.wave_id ORDER BY fdc.wave_id ASC
           )
           SELECT stakeholder_id, SUM(sum_wave_level_respondents) AS total_users_sum FROM base`;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [responsesRows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
      wave_id,
    ]);
    const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponsesFilters };
  }
}

/** 11) getTop3Packs */
export async function getTop3Packs(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, subcategory_id
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_l), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_segment") {
    const segment_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, subcategory_id
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                `;
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
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l DESC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 12) getBottom3Packs */
export async function getBottom3Packs(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, subcategory_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    GROUP BY stakeholder_id, subcategory_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_segment") {
    const segment_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, subcategory_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                `;
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
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                     rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 13) getTrendMovers */
export async function getTrendMovers(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, subcategory_id
                    )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                  ORDER BY ABS(f.delta_vs_prev_avg) DESC LIMIT 3
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    GROUP BY stakeholder_id, subcategory_id
                    )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                  ORDER BY ABS(f.delta_vs_prev_avg) DESC LIMIT 3
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_segment") {
    const segment_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, subcategory_id
                    )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                  ORDER BY ABS(f.delta_vs_prev_avg) DESC LIMIT 3
                `;
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
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, usf.subcategory_id
                    ORDER BY  usf.subcategory_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    WHERE wave_id = ?
                    )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                  ORDER BY ABS(f.delta_vs_prev_avg) DESC LIMIT 3
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 14) Get date range according to form n filters */
export async function getDateRange(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix === "stakeholder") {
    const company_id = params[0];
    const sql = `SELECT
                      usf.stakeholder_id,
                      fdc.wave_id,
                      CONCAT(
                          DATE_FORMAT(usfl.created_at, '%b %e, %Y'),
                          ' - ',
                          DATE_FORMAT(NOW(), '%b %e, %Y')
                      ) AS date_range
                      FROM form_data_company${company_id} AS fdc
                      INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                      LEFT JOIN user_stakeholder_form_logs usfl
                        ON usfl.user_stakeholder_form_id = fdc.form_id
                      AND usfl.wave = fdc.wave_id
                      WHERE usf.stakeholder_id = ?  
                        AND fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_segment") {
    const company_id = params[1];
    const sql = `SELECT
                      usf.stakeholder_id,
                      fdc.wave_id,
                      CONCAT(
                          DATE_FORMAT(usfl.created_at, '%b %e, %Y'),
                          ' - ',
                          DATE_FORMAT(NOW(), '%b %e, %Y')
                      ) AS date_range
                      FROM form_data_company${company_id} AS fdc
                      INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                      LEFT JOIN user_stakeholder_form_logs usfl
                        ON usfl.user_stakeholder_form_id = fdc.form_id
                      AND usfl.wave = fdc.wave_id
                      WHERE usf.stakeholder_id = ?  
                        AND fdc.wave_id = 1
                      ORDER BY usfl.created_at ASC LIMIT 1`;
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix === "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const sql = `WITH base AS (
                          SELECT
                            usf.stakeholder_id,
                            fdc.wave_id,
                            usf.current_wave,
                            MIN(usfl.created_at) AS created_at
                          FROM form_data_company${company_id} AS fdc
                          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                          LEFT JOIN user_stakeholder_form_logs usfl
                            ON usfl.user_stakeholder_form_id = fdc.form_id 
                          AND usfl.wave = fdc.wave_id
                          WHERE usf.stakeholder_id = ?
                          GROUP BY usf.stakeholder_id, fdc.wave_id
                          ORDER BY fdc.wave_id ASC
                        ),
                        waves AS (
                          SELECT
                            stakeholder_id,
                            wave_id,
                            created_at,
                            LEAD(created_at) OVER (PARTITION BY stakeholder_id ORDER BY wave_id) AS next_wave_date,
                            MAX(wave_id) OVER (PARTITION BY stakeholder_id) AS max_wave
                          FROM base
                        )
                        SELECT
                          w.stakeholder_id,
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
    const wave_id = params[0];
    const company_id = params[2];
    const sql = `WITH base AS (
                          SELECT
                            usf.stakeholder_id,
                            fdc.wave_id,
                            usf.current_wave,
                            MIN(usfl.created_at) AS created_at
                          FROM form_data_company${company_id} AS fdc
                          INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                          LEFT JOIN user_stakeholder_form_logs usfl
                            ON usfl.user_stakeholder_form_id = fdc.form_id 
                          AND usfl.wave = fdc.wave_id
                          WHERE usf.stakeholder_id = ?
                          GROUP BY usf.stakeholder_id, fdc.wave_id
                          ORDER BY fdc.wave_id ASC
                        ),
                        waves AS (
                          SELECT
                            stakeholder_id,
                            wave_id,
                            created_at,
                            LEAD(created_at) OVER (PARTITION BY stakeholder_id ORDER BY wave_id) AS next_wave_date,
                            MAX(wave_id) OVER (PARTITION BY stakeholder_id) AS max_wave
                          FROM base
                        )
                        SELECT
                          w.stakeholder_id,
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

/** 12) getBottom3Questions */
export async function getBottom3Questions(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id                    
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY  fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,                      
                      question_id,
                      three_word_outcome_en,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, question_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  -- LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                  LEFT JOIN question q1 ON q1.id = question_id
                  LEFT JOIN user_stakeholder_form usf1 ON q1.subcategory_id = usf1.subcategory_id AND q1.stakeholder_id = usf1.stakeholder_id AND q1.category_id = usf1.category_id AND usf1.company = ${company_id}
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,                      
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY  fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,    
                      question_id,
                      three_word_outcome_en,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    GROUP BY stakeholder_id, question_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  -- LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                  LEFT JOIN question q1 ON q1.id = question_id
                  LEFT JOIN user_stakeholder_form usf1 ON q1.subcategory_id = usf1.subcategory_id AND q1.stakeholder_id = usf1.stakeholder_id AND q1.category_id = usf1.category_id AND usf1.company = ${company_id}
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_segment") {
    const segment_id = params[0];
    const company_id = params[1];
    const baseSql = `WITH base AS (
                    SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,      
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id   
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                                                 
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY  fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,  
                      question_id,
                      three_word_outcome_en,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, question_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  -- LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                  LEFT JOIN question q1 ON q1.id = question_id
                  LEFT JOIN user_stakeholder_form usf1 ON q1.subcategory_id = usf1.subcategory_id AND q1.stakeholder_id = usf1.stakeholder_id AND q1.category_id = usf1.category_id AND usf1.company = ${company_id}
                `;
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
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
    const baseSql = `WITH base AS (
                   SELECT
                      usf.stakeholder_id,
                      usf.subcategory_id,
                      sub.name as subcategory_name,  
                      fdc.wave_id,
                      fdc.question_id,
                      q.three_word_outcome_en,
                      ROUND(AVG(fdc.option_numeric),2) AS avg_score
                    FROM form_data_company${company_id} AS fdc
                    INNER JOIN client_users AS cu ON cu.id = fdc.respondent_id
                    LEFT JOIN question q
                      ON q.id = fdc.question_id
                    INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                          
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
                    GROUP BY fdc.wave_id, fdc.question_id
                    ORDER BY  fdc.question_id ASC
                  ),
                scored AS (      
                    SELECT
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,
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
                      stakeholder_id,
                      subcategory_id,
                      subcategory_name,  
                      question_id,
                      three_word_outcome_en,
                      -- wave_id,
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    GROUP BY stakeholder_id, question_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                )
                  SELECT f.*, usf1.id AS form_id FROM final AS f
                  -- LEFT JOIN user_stakeholder_form usf1 ON f.subcategory_id = usf1.subcategory_id AND f.stakeholder_id = usf1.stakeholder_id AND usf1.company = ${company_id}
                  LEFT JOIN question q1 ON q1.id = question_id
                  LEFT JOIN user_stakeholder_form usf1 ON q1.subcategory_id = usf1.subcategory_id AND q1.stakeholder_id = usf1.stakeholder_id AND q1.category_id = usf1.category_id AND usf1.company = ${company_id}
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  }
}

/** 12) getBottom3Respondents */
export async function getBottom3Respondents(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
    const company_id = params[0];
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
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
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
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, respondent_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_wave") {
    const wave_id = params[0];
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
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
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
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    GROUP BY stakeholder_id, respondent_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  } else if (suffix == "stakeholder_segment") {
    const segment_id = params[0];
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
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                                                 
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
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
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_l END) AS rag_score_avg_l , -- ragscore from latest wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN rag_score_avg_p END) AS rag_score_avg_p , -- ragscore from previous wave
                      MAX(CASE WHEN wave_id = max_wave_for_q
                              THEN delta_vs_prev_avg_q END) AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored_with_max
                    GROUP BY stakeholder_id, respondent_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
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
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
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
                    INNER JOIN client_segment_users AS csu  
                        ON csu.segment_id = ? AND csu.company=?
                        AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                          
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id
                    WHERE usf.stakeholder_id = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
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
                      -- ROUND(AVG(rag_score_avg_q), 2) AS rag_score_avg,         -- avg across all waves for this subcategory
                      rag_score_avg_l, -- ragscore from latest wave
                      rag_score_avg_p, -- ragscore from previous wave
                      delta_vs_prev_avg_q AS delta_vs_prev_avg   -- delta from latest wave
                    FROM scored
                    GROUP BY stakeholder_id, respondent_id
                    ORDER BY rag_score_avg_l ASC LIMIT 3
                `;
    //console.log(baseSql);
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
      wave_id
    ]);
    //console.log(rows);
    return rows;
  }
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/** 2) All Waves */
export async function getAllWaves(company_id, stakeholder_id) {
  const sql = `SELECT DISTINCT CONCAT("Wave ",wave_id)as label, wave_id as value FROM \`wave_question_avg\` where company_id = ? AND stakeholder_id = ? ORDER BY wave_id ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [company_id, stakeholder_id]);
  return rows;
}

/** 3) All Segments */
export async function getAllSegments(company_id, stakeholder_id) {
  const sql = `
              SELECT DISTINCT 
                s.id AS value,
                s.name AS label
            FROM user_stakeholder_form usf
            LEFT JOIN segment s 
                ON FIND_IN_SET(s.id, usf.segments) > 0
            WHERE usf.company = ? AND usf.stakeholder_id = ?
            ORDER BY s.name ASC
              `;
  //console.log(sql);
  const [rows] = await pool.query(sql, [company_id, stakeholder_id]);
  return rows;
}

/** 2) getStakeholderDetails */
export async function getStakeholderDetails(stakeholder_id) {
  const sql = `SELECT name as stakeholder_name FROM \`stakeholder\` where id = ?`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [stakeholder_id]);
  return rows;
}
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++


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

/** 18) getAllPacks */
export async function getAllPacks(filters, stakeholder_id) {
  const { suffix, params } = pickVariant(filters);
  //
  if (suffix == "stakeholder") {
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
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ${company_id}
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
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
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [stakeholder_id]);
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
  } else if (suffix == "stakeholder_wave") {
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
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
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
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      stakeholder_id,
      company_id,
      wave_id
    ]);
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
  } else if (suffix == "stakeholder_segment") {
    const segment_id = params[0];
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
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
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
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [segment_id, company_id, stakeholder_id, company_id]);
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
  } else if (suffix == "stakeholder_wave_segment") {
    const wave_id = params[0];
    const segment_id = params[1];
    const company_id = params[2];
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
                    INNER JOIN client_segment_users AS csu  
                      ON csu.segment_id = ? AND csu.company=?
                      AND FIND_IN_SET(fdc.respondent_id, csu.client_users) > 0                     
                    INNER JOIN user_stakeholder_form usf ON fdc.form_id = usf.id AND usf.stakeholder_id = ?
                    LEFT JOIN subcategory sub ON sub.id = usf.subcategory_id
                    LEFT JOIN category cat ON usf.category_id = cat.id
                    WHERE usf.company = ?
                    AND fdc.option_numeric REGEXP '^[0-9]+$'
                    /*EXTRA_FILTERS*/
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
    const tableStr = `cu`;
    const { sql } = await applyExtraFilters(baseSql, filters, tableStr);
    //console.log(sql);
    const [rows] = await pool.query(sql, [
      segment_id,
      company_id,
      stakeholder_id,
      company_id,
      wave_id
    ]);
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
}