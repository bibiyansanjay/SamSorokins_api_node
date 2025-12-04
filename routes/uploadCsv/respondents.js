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
      "respondents",
      "RespondentMetaData_vF.xlsx"
    );

    if (!fs.existsSync(filePath)) {
      return res.status(400).json({
        success: false,
        message:
          "RespondentMetaData_vF.xlsx not found at /uploadCsv/respondents/",
      });
    }
    console.log("✅ File found:", filePath);
    // Read workbook
    const workbook = XLSX.readFile(filePath);
    const sheet = workbook.Sheets["DataSet for Kush"];
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

    const [[rowsSubcategory], [rowsCategory], [rowsStakeholder]] =
      await Promise.all([
        pool.query(sqlSubcategory),
        pool.query(sqlCategory),
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
    const subcategoryMap = rowsToMap(rowsSubcategory, "name", "id");
    //console.log("Subcategory Map:", subcategoryMap);
    const categoryMap = rowsToMap(rowsCategory, "name", "id");
    //console.log("Category Map:", categoryMap);
    const stakeholderMap = rowsToMap(rowsStakeholder, "name", "id");
    //console.log("Stakeholder Map:", stakeholderMap);
    // ---------- 4) Transform XLSX rows -> payload rows ----------
    const toInsert = [];
    const errors = [];

    xlsxRows.forEach((r, i) => {
      const packName = r["subcategory"] ?? "";
      const categoryName = r["category"] ?? "";
      const stakeholderName =
        r["Stakeholder Group"] ?? r["Stakeholder Group "] ?? "";

      const subcategory_id = subcategoryMap.get(norm(packName));
      const category_id = categoryMap.get(norm(categoryName));
      const stakeholder_id = stakeholderMap.get(norm(stakeholderName));

      if (!subcategory_id || !category_id || !stakeholder_id) {
        errors.push({
          row: i + 1,
          packName,
          categoryName,
          stakeholderName,
          missing: {
            subcategory_id: !subcategory_id,
            category_id: !category_id,
            stakeholder_id: !stakeholder_id,
          },
        });
      } else {
        toInsert.push([
          r["Name"] ?? "",
          r["Email"] ?? "",
          r["Organisation"] ?? "",
          r["Role"] ?? "",
          r["Region"] ?? "",
          stakeholder_id,
          subcategory_id,
          category_id,
          "2", //company_id
          "2", //created_by
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
        "name",
        "email",
        "organization",
        "role",
        "region",
        "stakeholder_id ",
        "subcategory_id",
        "category_id",
        "company",
        "created_by",
        "isActive",
      ];
      const sql = `
      INSERT INTO client_users
      (${cols.join(", ")})
      VALUES ${buildMultiValuesPlaceholders(toInsert.length, cols.length)}
    `;
      const flatParams = toInsert.flat();
      try {
        const [insertResult] = await pool.query(sql, flatParams);

        return res.status(200).json({
          success: true,
          message: `Inserted ${insertResult.affectedRows} client users successfully`,
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
