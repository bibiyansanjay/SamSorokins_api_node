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
      "templates",
      // "questionFile.xlsx"
      "Templates.xlsx"
    );

    if (!fs.existsSync(filePath)) {
      return res.status(400).json({
        success: false,
        message: "Templates.xlsx not found at /uploadCsv/templates/",
      });
    }
    console.log("✅ File found:", filePath);
    // Read workbook
    const workbook = XLSX.readFile(filePath);
    const sheet = workbook.Sheets["Templates"];
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
    const sqlStakeholder = `SELECT id, name from stakeholder ORDER BY id ASC`;

    const [[rowsStakeholder]] = await Promise.all([
      pool.query(sqlStakeholder),
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
    const stakeholderMap = rowsToMap(rowsStakeholder, "name", "id");
    //console.log("Category Map:", categoryMap);
    //console.log("Question Type Map:", questionTypeMap);
    // ---------- 4) Transform XLSX rows -> payload rows ----------
    // XLSX headers you showed:
    // 'Pack Name', 'Category', 'Current Stakeholder ', 'Question Type',
    // 'Action Oriented Outcome', 'Question', 'Response Options', 'Positive Polarity'

    const toInsert = [];
    const errors = [];

    xlsxRows.forEach((r, i) => {
      const stakeholderName = r["StakeholderGroup"] ?? "";

      const stakeholder_id = stakeholderMap.get(norm(stakeholderName));

      if (!stakeholder_id) {
        errors.push({
          row: i + 1,
          stakeholderName,
          missing: {
            stakeholder_id: !stakeholder_id,
          },
        });
      } else {
        toInsert.push([
          stakeholder_id,
          r["Language"] ?? "", //Language
          r["Tone"] ?? "", //Tone
          r["SubjectLine"] ?? "", //SubjectLine
          r["MessageBody"] ?? "", //MessageBody
          "6", //created_by
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
        "stakeholder_id",
        "language",
        "tone",
        "subject",
        "body",
        "created_by",
      ];
      const sql = `
      INSERT INTO template
      (${cols.join(", ")})
      VALUES ${buildMultiValuesPlaceholders(toInsert.length, cols.length)}
    `;
      const flatParams = toInsert.flat();
      try {
        const [insertResult] = await pool.query(sql, flatParams);

        return res.status(200).json({
          success: true,
          message: `Inserted ${insertResult.affectedRows} templates successfully`,
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
