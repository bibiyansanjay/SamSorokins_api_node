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
      suffix: "stakeholderOverView",
      params: [company_id, city, region, country],
    };
  if (wave_id && !segment_id && company_id)
    return {
      suffix: "stakeholderOverView_wave",
      params: [wave_id, company_id, city, region, country],
    };
  if (!wave_id && segment_id && company_id)
    return {
      suffix: "stakeholderOverView_segment",
      params: [segment_id, company_id, city, region, country],
    };
  if (wave_id && segment_id && company_id)
    return {
      suffix: "stakeholderOverView_wave_segment",
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
  if (suffix == "stakeholderOverView") {
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
  } else if (suffix == "stakeholderOverView_wave") {
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
  } else if (suffix == "stakeholderOverView_segment") {
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
  } else if (suffix == "stakeholderOverView_wave_segment") {
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
  if (suffix == "stakeholderOverView") {
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
  } else if (suffix == "stakeholderOverView_wave") {
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
  } else if (suffix == "stakeholderOverView_segment") {
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
  } else if (suffix == "stakeholderOverView_wave_segment") {
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
  if (suffix == "stakeholderOverView") {
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
  } else if (suffix == "stakeholderOverView_wave") {
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
  } else if (suffix == "stakeholderOverView_segment") {
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
  } else if (suffix == "stakeholderOverView_wave_segment") {
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
  if (suffix == "stakeholderOverView") {
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
  } else if (suffix == "stakeholderOverView_wave") {
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
  } else if (suffix == "stakeholderOverView_segment") {
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
  } else if (suffix == "stakeholderOverView_wave_segment") {
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
  if (suffix == "stakeholderOverView") {
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
    //console.log(responsesRows);
    const totalResponsesFilters = responsesRows?.[0]?.total_users_sum || 0;
    return { totalResponsesFilters };
  } else if (suffix == "stakeholderOverView_wave") {
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
  } else if (suffix == "stakeholderOverView_segment") {
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
  } else if (suffix == "stakeholderOverView_wave_segment") {
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
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/** 1) All Stakeholders */
export async function getAllStakeholders(company_id) {
  const sql = `
              SELECT DISTINCT 
                stk.id,
                stk.name
            FROM user_stakeholder_form usf
            LEFT JOIN stakeholder stk
                ON stk.id = usf.stakeholder_id
            WHERE usf.company = ?
            ORDER BY stk.name ASC
              `;
  //console.log(sql);
  const [rows] = await pool.query(sql, [company_id]);
  return rows;
}

/** 2) All Waves */
export async function getAllWaves(company_id) {
  const sql = `SELECT DISTINCT CONCAT("Wave ",wave_id)as label, wave_id as value FROM \`wave_question_avg\` where company_id = ? ORDER BY wave_id ASC`;
  //console.log(sql);
  const [rows] = await pool.query(sql, [company_id]);
  return rows;
}

/** 3) All Segments */
export async function getAllSegments(company_id) {
  const sql = `
              SELECT DISTINCT 
                s.id AS value,
                s.name AS label
            FROM user_stakeholder_form usf
            LEFT JOIN segment s 
                ON FIND_IN_SET(s.id, usf.segments) > 0
            WHERE usf.company = ?
            ORDER BY s.name ASC
              `;
  //console.log(sql);
  const [rows] = await pool.query(sql, [company_id]);
  return rows;
}

/** 4) getAllClassificationAndStakeholders */
export async function getAllClassificationAndStakeholders(company_id) {
  const sql = `
              SELECT 
                stk.classification,
                GROUP_CONCAT(DISTINCT stk.id ORDER BY stk.id) AS ids,
                stk.image,
                stk.image_name
            FROM user_stakeholder_form usf
            LEFT JOIN stakeholder stk 
                ON stk.id = usf.stakeholder_id
            WHERE usf.company = ?
            GROUP BY stk.classification
            ORDER BY stk.classification ASC
              `;
  //console.log(sql);
  const [rows] = await pool.query(sql, [company_id]);
  return rows;
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
