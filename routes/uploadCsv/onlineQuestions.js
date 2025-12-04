import pool from "../../db";
import multer from "multer";
import { Readable } from "stream";
import csvParser from "csv-parser"; // npm install csv-parser

// In-memory upload
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

// Helpers
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

// Main API
export default async (req, res) => {
  try {
    const userRole = req.user.role;
    if (userRole != "1" && userRole != "6") {
      return res.status(403).json({
        success: false,
        message: "Unauthorized Access",
      });
    }

    var clientID = null;
    var company = null;
    // Get user type from query parameters
    //console.log("req.query.usertype", req.query.usertype);
    const userType = req.query.usertype ? req.query.usertype : undefined;
    //console.log("userType", userType);
    if (!userType) {
      return res.status(400).json({
        success: false,
        message: "User type parameter is required",
      });
    }
    if (userType === "platform") {
      clientID = 6;
    } else if (userType === "client") {
      // Get client ID from query parameters
      clientID = req.query.client ? Number(req.query.client) : undefined;

      if (!clientID) {
        return res.status(400).json({
          success: false,
          message: "client ID parameter is required",
        });
      }
      company = clientID;
    } else {
      return res.status(400).json({
        success: false,
        message: "User type invalid",
      });
    }
    //console.log("clientID", clientID);
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
      // console.log("CSV File received:", req.file.originalname);

      // --- Step 1: Parse CSV in-memory ---
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

      console.log(`Parsed ${csvRows.length} rows`);

      // --- Step 2: Fetch lookup data in parallel ---
      const [
        [rowsSubcategory],
        [rowsCategory],
        [rowsStakeholder],
        [rowsQuestionType],
        [rowsKPI],
      ] = await Promise.all([
        pool.query("SELECT id, name FROM subcategory"),
        pool.query("SELECT id, name FROM category"),
        pool.query("SELECT id, name FROM stakeholder"),
        pool.query("SELECT id, name FROM question_type"),
        pool.query("SELECT id, name FROM kpi"),
      ]);

      // --- Step 3: Build lookup maps ---
      const subcategoryMap = rowsToMap(rowsSubcategory);
      const categoryMap = rowsToMap(rowsCategory);
      const stakeholderMap = rowsToMap(rowsStakeholder);
      const questionTypeMap = rowsToMap(rowsQuestionType);
      const kpiMap = rowsToMap(rowsKPI);

      // --- Step 4: Transform rows ---
      const toInsert = [];
      const errors = [];

      csvRows.forEach((r, i) => {
        const subcategory_id = subcategoryMap.get(norm(r["Pack Name"] ?? ""));
        const category_id = categoryMap.get(norm(r["Category"] ?? ""));
        const stakeholder_id = stakeholderMap.get(
          norm(r["Current Stakeholder"] ?? r["Current Stakeholder "] ?? "")
        );
        const question_type_id = questionTypeMap.get(
          norm(r["Question Type"] ?? "")
        );
        const kpi_id = kpiMap.get(norm(r["KPI"] ?? ""));

        if (
          !subcategory_id ||
          !category_id ||
          !stakeholder_id ||
          !question_type_id ||
          !kpi_id
        ) {
          errors.push({
            row: i + 1,
            packName: r["Pack Name"],
            categoryName: r["Category"],
            stakeholderName: r["Current Stakeholder"],
            qTypeName: r["Question Type"],
            kpiName: r["KPI"],
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
            category_id,
            r["Positive Polarity"] ?? "",
            r["Priority Level"] ?? "",
            clientID, //"6", // created_by
            company, //"6", // company
            "1", // isActive
            r["Question ID"] ?? "",
            r["Reinforces Question ID 1"] ?? "",
            r["Reinforcement Type 1"] ?? "",
            r["Reinforces Question ID 2"] ?? "",
            r["Reinforcement Type 2"] ?? "",
          ]);
        }
      });

      // --- Step 5: Handle validation errors ---
      if (errors.length > 0) {
        console.warn("Validation errors found:", errors.length);
        return res.status(400).json({
          success: false,
          message:
            "Validation failed. Some rows have unmatched lookup values. No data inserted.",
          errors,
        });
      }

      // --- Step 6: Bulk Insert ---
      const cols = [
        "question_type",
        "question_text_en",
        "question_text_nl",
        "answer_options_en",
        "answer_options_nl",
        "three_word_outcome_en",
        "stakeholder_id",
        "subcategory_id",
        "kpi_id",
        "category_id",
        "polarity",
        "priority",
        "created_by",
        "company",
        "isActive",
        "upload_q_id",
        "upload_r_q_id1",
        "reinforcement_type1",
        "upload_r_q_id2",
        "reinforcement_type2",
      ];

      const sql = `
        INSERT INTO question (${cols.join(", ")})
        VALUES ${buildMultiValuesPlaceholders(toInsert.length, cols.length)};
      `;

      const flatParams = toInsert.flat();
      try {
        const [insertResult] = await pool.query(sql, flatParams);

        console.log(`Inserted ${insertResult.affectedRows} questions`);

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
    });
  } catch (error) {
    console.error("Upload error:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while uploading questions",
      error: error.message,
    });
  }
};
