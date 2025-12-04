import pool from "../../db";

// Function to run Query 1 (getData)
const getData = async (stakeholderFormId) => {
  //
  // Step 1: Get the WaveAvgRagNDelta for the given stakeholderFormId
  const getWaveAvgRagNDelta = async (id) => {
    const sql = `SELECT wqa.wave_id, ROUND(AVG(wqa.average), 2) AS avg_rag, ROUND(AVG(wqa.delta_vs_prev_avg), 2) AS delta_vs_prev_avg, wqa.company_id
                        FROM \`wave_question_avg\` wqa               
                        WHERE wqa.form_id = ? AND wqa.question_type != "single_select"
                        GROUP BY wqa.wave_id 
                        ORDER BY wqa.wave_id DESC`;
    const [rows] = await pool.query(sql, [id]);
    //console.log("rows:", rows);
    return rows; // Assuming the result will be in rows[0].company
  };

  // Step 2: Generate and execute the dynamic query based on the company name
  const getDynamicQuery = async (id) => {
    const waveAvgRagNDelta = await getWaveAvgRagNDelta(id); // Step 1: Get the waveAvgRagNDelta value
    console.log("waveAvgRagNDelta", waveAvgRagNDelta);
    const company = waveAvgRagNDelta[0].company_id;
    if (!waveAvgRagNDelta) {
      throw new Error("waveAvgRagNDelta not found");
    }

    // Construct the dynamic query
    const dynamicQuery = `
                        WITH AvgScores AS (
                            SELECT 
                                wave_id,
                                respondent_id,
                                ROUND(AVG(option_numeric),2) AS avg_score
                            FROM \`form_data_company${company}\`
                            WHERE form_id=? AND option_numeric REGEXP '^[0-9]+$'
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
                            avg_score,
                            -- Calculate the percentage for each category using conditional aggregation
                            (COUNT(CASE WHEN response_category = 'Amber' THEN 1 END) * 100.0 / COUNT(*)) AS amber_percentage,
                            (COUNT(CASE WHEN response_category = 'Red' THEN 1 END) * 100.0 / COUNT(*)) AS red_percentage,
                            (COUNT(CASE WHEN response_category = 'Green' THEN 1 END) * 100.0 / COUNT(*)) AS green_percentage
                        FROM ClassifiedResponses
                        GROUP BY wave_id
                        ORDER BY wave_id DESC`;
    // Execute the dynamic query
    const [result] = await pool.query(dynamicQuery, [id]); // Execute the dynamic query
    //return result;
    return { waveAvgRagNDelta, result };
  };

  try {
    // Step 3: Get the data from the dynamic query
    const result = await getDynamicQuery(stakeholderFormId);
    //console.log("Respondents Data: ", result); // Log the result for debugging

    // Return the result in the desired format
    return { result };
  } catch (err) {
    console.error("Error fetching respondents data:", err);
    return { result: "" }; // Return an empty array in case of error
  }
};

export default async (req, res, next) => {
  try {
    const stakeholderFormId = 24;

    // Run all queries concurrently using Promise.all
    const results = await Promise.all([getData(stakeholderFormId)]);

    // Prepare the output object
    const output = {
      data: results[0]?.result || [],
    };

    // Return the result
    return res.status(200).json({
      success: true,
      message: "RagTrend data fetched successfully",
      data: output,
    });
  } catch (error) {
    console.error("GET /RagTrend - Error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching RagTrend data",
      error: error.message,
    });
  }
};
