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
      // "questionFile.xlsx"
      "PC-010_Strategic Workforce Planning Assessment.xlsx"
    );

    if (!fs.existsSync(filePath)) {
      return res.status(400).json({
        success: false,
        message: "questionFile.xlsx not found at /uploadCsv/questions/",
      });
    }
    console.log("✅ File found:", filePath);
    // Read workbook
    const workbook = XLSX.readFile(filePath);
    const sheet = workbook.Sheets["questions"];
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

    const [
      [rowsSubcategory],
      [rowsCategory],
      [rowsStakeholder],
      [rowsQuestionType],
    ] = await Promise.all([
      pool.query(sqlSubcategory),
      pool.query(sqlCategory),
      pool.query(sqlStakeholder),
      pool.query(sqlQuestionType),
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

      const subcategory_id = subcategoryMap.get(norm(packName));
      const category_id = categoryMap.get(norm(categoryName));
      const stakeholder_id = stakeholderMap.get(norm(stakeholderName));
      const question_type_id = questionTypeMap.get(norm(qTypeName));

      if (
        !subcategory_id ||
        !category_id ||
        !stakeholder_id ||
        !question_type_id
      ) {
        errors.push({
          row: i + 1,
          packName,
          categoryName,
          stakeholderName,
          qTypeName,
          missing: {
            subcategory_id: !subcategory_id,
            category_id: !category_id,
            stakeholder_id: !stakeholder_id,
            question_type_id: !question_type_id,
          },
        });
      } else {
        toInsert.push([
          question_type_id,
          r["Question"] ?? "",
          r["Response Options"] ?? "",
          r["Action Oriented Outcome"] ?? "",
          stakeholder_id,
          subcategory_id,
          // category_id, // not used in table
          r["Positive Polarity"] ?? "",
          "6", //company
          "1", //isActive
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
        "answer_options_en",
        "three_word_outcome_en",
        "stakeholder_id",
        "subcategory_id",
        "polarity",
        "created_by",
        "isActive",
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
