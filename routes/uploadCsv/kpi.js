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
      "kpi",
      // "questionFile.xlsx"
      "Kpi.xlsx"
    );

    if (!fs.existsSync(filePath)) {
      return res.status(400).json({
        success: false,
        message: "Kpi.xlsx not found at /uploadCsv/kpi/",
      });
    }
    console.log("✅ File found:", filePath);
    // Read workbook
    const workbook = XLSX.readFile(filePath);
    const sheet = workbook.Sheets["KPI"];
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

    //////////////////////////////////////////////////////////////////////////////

    const toInsert = [];

    xlsxRows.forEach((r, i) => {
      toInsert.push([
        r["KPI_Code"] ?? "",
        "6", //created_by
        "1", //isActive
      ]);
    });
    console.log(toInsert);

    console.log("All data is valid. Proceeding with insertion...");
    //
    // 5) Insert all rows in one SQL
    const cols = ["name", "created_by", "isActive"];
    const sql = `
      INSERT INTO kpi
      (${cols.join(", ")})
      VALUES ${buildMultiValuesPlaceholders(toInsert.length, cols.length)}
    `;
    const flatParams = toInsert.flat();
    try {
      const [insertResult] = await pool.query(sql, flatParams);

      return res.status(200).json({
        success: true,
        message: `Inserted ${insertResult.affectedRows} kpi's successfully`,
      });
    } catch (e) {
      //await conn.rollback();
      return res.status(500).json({
        success: false,
        message: "Bulk insert failed",
        error: e.message,
      });
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
