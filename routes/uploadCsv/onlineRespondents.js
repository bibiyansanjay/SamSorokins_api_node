import pool from "../../db";
import multer from "multer";
import { Readable } from "stream";
import csvParser from "csv-parser"; // npm install csv-parser

// Use in-memory storage
const upload = multer({
  storage: multer.memoryStorage(),
  fileFilter: (req, file, cb) => {
    if (file.mimetype === "text/csv" || file.originalname.endsWith(".csv")) {
      cb(null, true);
    } else {
      cb(new Error("Only CSV files are allowed"));
    }
  },
}).single("file");

// Utility functions
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

// API Route
export default async (req, res) => {
  // Authorization check
  const userRole = req.user.role;
  if (userRole != "1" && userRole != "6") {
    return res.status(403).json({
      success: false,
      message: "Unauthorized Access",
    });
  }
  // Get client ID from query parameters
  const clientID = req.query.client ? Number(req.query.client) : undefined;
  // console.log("clientID", clientID);
  if (!clientID) {
    return res.status(400).json({
      success: false,
      message: "client ID parameter is required",
    });
  }

  upload(req, res, async (err) => {
    if (err) {
      console.error("Upload error:", err);
      return res.status(400).json({ success: false, message: err.message });
    }

    if (!req.file) {
      return res.status(400).json({
        success: false,
        message: "No file uploaded. Please upload a CSV file.",
      });
    }
    //console.log("File received in memory:", req.file.originalname);

    try {
      // --- Step 1: Parse CSV directly from memory buffer ---
      //console.time("READ_CSV");
      const csvRows = [];
      await new Promise((resolve, reject) => {
        const stream = Readable.from(req.file.buffer);
        stream
          .pipe(csvParser())
          .on("data", (row) => {
            const cleaned = {};
            for (const key of Object.keys(row)) {
              cleaned[key.trim()] = (row[key] ?? "").toString().trim();
            }
            csvRows.push(cleaned);
          })
          .on("end", resolve)
          .on("error", reject);
      });
      //console.timeEnd("READ_CSV");
      //console.log(`Parsed ${csvRows.length} rows from in-memory CSV`);

      // --- Step 2: Fetch DB lookup tables in parallel ---
      //console.time("DB_MAPS");
      const [[rowsSubcategory], [rowsCategory], [rowsStakeholder]] =
        await Promise.all([
          pool.query("SELECT id, name FROM subcategory"),
          pool.query("SELECT id, name FROM category"),
          pool.query("SELECT id, name FROM stakeholder"),
        ]);
      //console.timeEnd("DB_MAPS");

      // --- Step 3: Build lookup maps ---
      const subcategoryMap = rowsToMap(rowsSubcategory);
      const categoryMap = rowsToMap(rowsCategory);
      const stakeholderMap = rowsToMap(rowsStakeholder);

      // --- Step 4: Transform CSV rows to SQL insert format ---
      //console.time("TRANSFORM");
      const toInsert = [];
      const errors = [];

      csvRows.forEach((r, i) => {
        const subcategory_id = subcategoryMap.get(norm(r["subcategory"] ?? ""));
        const category_id = categoryMap.get(norm(r["category"] ?? ""));
        const stakeholder_id = stakeholderMap.get(
          norm(r["Stakeholder Group"] ?? r["Stakeholder Group "] ?? "")
        );

        if (!subcategory_id || !category_id || !stakeholder_id) {
          errors.push({
            row: i + 1,
            subcategory: r["subcategory"],
            category: r["category"],
            stakeholder: r["Stakeholder Group"],
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
            clientID, //"2", // company_id
            clientID, //"2", // created_by
            "1", // isActive
          ]);
        }
      });
      //console.timeEnd("TRANSFORM");

      if (errors.length > 0) {
        console.warn("Validation errors found:", errors.length);
        return res.status(400).json({
          success: false,
          message:
            "Validation failed. Some rows have unmatched lookup values. No data inserted.",
          errors,
        });
      }

      // --- Step 5: Bulk insert into MySQL ---
      //console.time("INSERT");
      const cols = [
        "name",
        "email",
        "organization",
        "role",
        "region",
        "stakeholder_id",
        "subcategory_id",
        "category_id",
        "company",
        "created_by",
        "isActive",
      ];

      const sql = `
        INSERT INTO client_users (${cols.join(", ")})
        VALUES ${buildMultiValuesPlaceholders(toInsert.length, cols.length)};
      `;

      const flatParams = toInsert.flat();
      const [insertResult] = await pool.query(sql, flatParams);
      //console.timeEnd("INSERT");

      console.log(`Inserted ${insertResult.affectedRows} client users`);

      return res.status(200).json({
        success: true,
        message: `Inserted ${insertResult.affectedRows} client users successfully`,
      });
    } catch (error) {
      console.error("Error processing CSV:", error);
      if (error.code === "ER_DUP_ENTRY") {
        // Generic duplicate entry fallback
        return res.status(409).json({
          success: false,
          message: "Failed to process CSV file: Duplicate email",
          error: error.message,
        });
      } else {
        console.error("Failed to process CSV file:", error.message);
        return res.status(500).json({
          success: false,
          message: "Failed to process CSV file",
          error: error.message,
        });
      }
    }
  });
};
