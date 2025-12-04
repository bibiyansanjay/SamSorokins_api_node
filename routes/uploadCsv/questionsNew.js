import pool from "../../db";
import fs from "fs";
import path from "path";
import * as XLSX from "xlsx";

const norm = (s) =>
  (s ?? "").toString().trim().replace(/\s+/g, " ").toLowerCase();

function rowsToMap(rows, key = "name", val = "id") {
  const m = new Map();
  for (const r of rows || []) {
    if (r?.[key] != null) m.set(norm(r[key]), r[val]);
  }
  return m;
}

function buildMultiValuesPlaceholders(rowCount, colsPerRow) {
  const one = `(${Array(colsPerRow).fill("?").join(",")})`;
  return Array(rowCount).fill(one).join(",");
}

export default async (req, res) => {
  try {
    // ---------- Check for the file ----------
    const filePath = path.join(
      process.cwd(),
      "uploadCsv",
      "questions",
      "FinalDataSetKushv3.xlsx"
    );

    if (!fs.existsSync(filePath)) {
      return res.status(400).json({
        success: false,
        message: "Questions.xlsx not found at /uploadCsv/questions/",
      });
    }
    console.log("✅ File found:", filePath);
    // Read workbook
    const workbook = XLSX.readFile(filePath);
    const sheet = workbook.Sheets["UXS"];
    // Convert to JSON
    const jsonData = XLSX.utils.sheet_to_json(sheet);
    //console.log("📄 File content:", jsonData);

    const xlsxRows = jsonData.map((row) => {
      const cleaned = {};
      for (const k of Object.keys(row)) {
        cleaned[k.trim()] = row[k];
      }
      return cleaned;
    });

    // ---------- Your existing DB queries ----------
    const sqlSubcategory = `SELECT id, name from subcategory ORDER BY id ASC`;
    const sqlCategory = `SELECT id, name from category ORDER BY id ASC`;
    const sqlStakeholder = `SELECT id, name from stakeholder ORDER BY id ASC`;
    const sqlQuestionType = `SELECT id, name from question_type ORDER BY id ASC`;
    const sqlKPI = `SELECT id, name from kpi ORDER BY id ASC`;

    const [
      [rowsSubcategory],
      [rowsCategory],
      [rowsStakeholder],
      [rowsQuestionType],
      [rowsKPI],
    ] = await Promise.all([
      pool.query(sqlSubcategory),
      pool.query(sqlCategory),
      pool.query(sqlStakeholder),
      pool.query(sqlQuestionType),
      pool.query(sqlKPI),
    ]).catch((error) => {
      console.error("Error fetching data:", error);
      res.status(500).json({
        success: false,
        message: "An error occurred while fetching data",
        error: error.message,
      });
    });
    //////////////////////////////////////////////////////////////////////////////
    // ---------- 3) Build name->id maps ----------
    const subcategoryMap = rowsToMap(rowsSubcategory, "name", "id");
    //console.log("Subcategory Map:", subcategoryMap);
    const categoryMap = rowsToMap(rowsCategory, "name", "id");
    //console.log("Category Map:", categoryMap);
    const stakeholderMap = rowsToMap(rowsStakeholder, "name", "id");
    //console.log("Stakeholder Map:", stakeholderMap);
    const questionTypeMap = rowsToMap(rowsQuestionType, "name", "id");
    //console.log("Question Type Map:", questionTypeMap);
    const kpiMap = rowsToMap(rowsKPI, "name", "id");
    //console.log("KPI Map:", kpiMap);
    // ---------- 4) Transform XLSX rows -> payload rows ----------
    // XLSX headers you showed:
    // 'Pack Name', 'Category', 'Current Stakeholder ', 'Question Type',
    // 'Action Oriented Outcome', 'Question', 'Response Options', 'Positive Polarity'

    const toInsert = [];
    const errors = [];

    xlsxRows.forEach((r, i) => {
      const packName = r["Pack Name"] ?? "";
      const categoryName = r["Category"] ?? "";
      const stakeholderName =
        r["Current Stakeholder"] ?? r["Current Stakeholder "] ?? "";
      const qTypeName = r["Question Type"] ?? "";
      const kpiName = r["KPI"] ?? "";

      const subcategory_id = subcategoryMap.get(norm(packName));
      const category_id = categoryMap.get(norm(categoryName));
      const stakeholder_id = stakeholderMap.get(norm(stakeholderName));
      const question_type_id = questionTypeMap.get(norm(qTypeName));
      const kpi_id = kpiMap.get(norm(kpiName));

      if (
        !subcategory_id ||
        !category_id ||
        !stakeholder_id ||
        !question_type_id ||
        !kpi_id
      ) {
        errors.push({
          row: i + 1,
          packName,
          categoryName,
          stakeholderName,
          qTypeName,
          kpiName,
          missing: {
            subcategory_id: !subcategory_id,
            category_id: !category_id,
            stakeholder_id: !stakeholder_id,
            question_type_id: !question_type_id,
            kpi_id: !kpi_id,
          },
        });
      } else {
        toInsert.push([
          question_type_id,
          r["Question"] ?? "",
          r["Question NL"] ?? "",
          r["Response Options"] ?? "",
          r["Response Options NL"] ?? "",
          r["Action Oriented Outcome"] ?? "",
          stakeholder_id,
          subcategory_id,
          kpi_id,
          category_id, // not used in table
          r["Positive Polarity"] ?? "",
          r["Priority Level"] ?? "",
          "6", //created_by
          "1", //isActive
          r["Question ID"] ?? "",
          r["Reinforces Question ID 1"] ?? "",
          r["Reinforcement Type 1"] ?? "",
          r["Reinforces Question ID 2"] ?? "",
          r["Reinforcement Type 2"] ?? "",
        ]);
      }
    });
    console.log(toInsert);
    console.log(errors);
    // 4) Strict validation: if any row has errors, stop
    if (errors.length > 0) {
      return res.status(400).json({
        success: false,
        message:
          "Validation failed. Some rows have unmatched lookup values. No data inserted.",
        errors,
      });
    } else {
      console.log("All data is valid. Proceeding with insertion...");
      //
      // 5) Insert all rows in one SQL
      const cols = [
        "question_type",
        "question_text_en",
        "question_text_nl",
        "answer_options_en",
        "answer_options_nl",
        "three_word_outcome_en",
        //"three_word_outcome_nl",
        "stakeholder_id",
        "subcategory_id",
        "kpi_id",
        "category_id",
        "polarity",
        "priority",
        "created_by",
        "isActive",
        "upload_q_id",
        "upload_r_q_id1",
        "reinforcement_type1",
        "upload_r_q_id2",
        "reinforcement_type2",
      ];
      const sql = `
      INSERT INTO question
      (${cols.join(", ")})
      VALUES ${buildMultiValuesPlaceholders(toInsert.length, cols.length)}
    `;
      const flatParams = toInsert.flat();
      try {
        const [insertResult] = await pool.query(sql, flatParams);

        return res.status(200).json({
          success: true,
          message: `Inserted ${insertResult.affectedRows} questions successfully`,
        });
      } catch (e) {
        //await conn.rollback();
        return res.status(500).json({
          success: false,
          message: "Bulk insert failed",
          error: e.message,
        });
      }
    }
  } catch (error) {
    console.error(
      "An error occurred while uploading data. Please try again later.",
      error
    );
    return res.status(500).json({
      success: false,
      message:
        "An error occurred while uploading data. Please try again later.",
      error: error.message,
    });
  }
};
