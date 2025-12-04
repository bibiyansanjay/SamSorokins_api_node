const pool = require("../db");
const axios = require("axios");
import updateRecord from "./../utils/updateRecord";

async function sendSubmissionIdToLambda() {
  //console.log("sendSubmissionIdToLambda job running...");

  const conn = await pool.getConnection();
  try {
    const [processingRows] = await conn.query(
      `SELECT id FROM user_submissions WHERE status = 'PROCESSING' LIMIT 1`
    );
    if (processingRows.length > 0) {
      console.log("A PROCESSING record exists. Skipping...");
      return;
    }

    const [pendingRows] = await conn.query(
      `SELECT id, submission_id, retry_count FROM user_submissions WHERE status = 'PENDING' AND retry_count < 3 ORDER BY created_at ASC LIMIT 1`
    );
    if (pendingRows.length === 0) {
      console.log("No PENDING records found.");
      return;
    }

    const { submission_id } = pendingRows[0];
    const id = pendingRows[0].id;
    const retry_count = pendingRows[0].retry_count;
    try {
      //)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
      const markProcessingResult = await updateRecord(
        "user_submissions",
        {
          status: "PROCESSING",
        },
        {
          id: id,
        }
      );
      if (markProcessingResult.affectedRows === 0) {
        console.warn("marking PROCESSING failed for ID:", id);
        return;
      }
      //)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
      //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      const response = await axios.post(
        process.env.LAMBDA_URL_USER_SUBMISSION,
        { submission_id }, // 🔁 Send in request body
        { timeout: 3000 }
      );
      // console.log("Sent to Lambda:", { submission_id });
      // console.log("response:", response);
      // console.log("response data:", response.data);
      //console.log("response status:", response.status);
      if (
        response.status == 200 &&
        response.data?.message == "Submission received"
      ) {
        console.log("Lambda responded with success:", response.data);
        //delete the record now
        const sql = `DELETE FROM \`user_submissions\` WHERE id = ?`;
        //console.log("sql: ", sql);
        const [result] = await pool.query(sql, [id]);
        //console.log("result :", result);
        if (result.affectedRows === 0) {
          console.warn("Delete failed for user_submissions:", id);
        }
      } //Next wave is started user can not submit old wave form
      //Form already submitted by user
      else if (
        response.status == 200 &&
        response.data?.message == "Form already submitted by user"
      ) {
        console.log("Form already submitted by user");
        // Update status++++++++++++++++++++++++++starts
        try {
          const retryResult = await updateRecord(
            "user_submissions",
            {
              status: "SUBMITTED TWICE",
            },
            {
              id: id,
            }
          );
          if (retryResult.affectedRows === 0) {
            console.warn("Retry update failed for ID:", id);
          }
        } catch (retryErr) {
          console.error(
            "Error updating retry after Lambda exception:",
            retryErr.message
          );
        }
        // Update status++++++++++++++++++++++++++ends
      } else if (
        response.status == 200 &&
        response.data?.message ==
          "Next wave is started user can not submit old wave form"
      ) {
        console.log("Next wave is started user can not submit old wave form");
        // Update status++++++++++++++++++++++++++starts
        try {
          const retryResult = await updateRecord(
            "user_submissions",
            {
              status: "OLD WAVE SUBMISSION",
            },
            {
              id: id,
            }
          );
          if (retryResult.affectedRows === 0) {
            console.warn("Retry update failed for ID:", id);
          }
        } catch (retryErr) {
          console.error(
            "Error updating retry after Lambda exception:",
            retryErr.message
          );
        }
        // Update status++++++++++++++++++++++++++ends
      } else {
        console.log(response.data);
        console.warn(
          "Lambda responded, but not success:",
          response.data?.error
        );
        const newRetryCount = retry_count + 1;
        // Determine next status
        const nextStatus = newRetryCount >= 3 ? "FAILED" : "PENDING";
        // Update retry and status++++++++++++++++++++++++++starts
        try {
          const result = await updateRecord(
            "user_submissions",
            {
              retry_count: newRetryCount,
              status: nextStatus, // Optional: reset status back to PENDING
            },
            {
              id: id,
            }
          );
          if (result.affectedRows === 0) {
            console.warn("Retry update failed for ID:", id);
          }
        } catch (retryErr) {
          console.error(
            "Error updating retry after Lambda exception:",
            retryErr.message
          );
        }
        // Update retry and status++++++++++++++++++++++++++ends
        //
      }
      //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    } catch (err) {
      console.error("Error sending to Lambda:", err.message);
      // Fallback: Always update retry_count in case of failure
      const newRetryCount = retry_count + 1;
      // Determine next status
      const nextStatus = newRetryCount >= 3 ? "FAILED" : "PENDING";
      // Update retry and status++++++++++++++++++++++++++starts
      try {
        const retryResult = await updateRecord(
          "user_submissions",
          {
            retry_count: newRetryCount,
            status: nextStatus, // Optional: reset status back to PENDING
          },
          {
            id: id,
          }
        );
        if (retryResult.affectedRows === 0) {
          console.warn("Retry update failed for ID:", id);
        }
      } catch (retryErr) {
        console.error(
          "Error updating retry after Lambda exception:",
          retryErr.message
        );
      }
    }
  } catch (error) {
    console.error("sendSubmissionIdToLambda error:", error.message);
  } finally {
    if (conn) conn.release();
  }
}

module.exports = sendSubmissionIdToLambda;
