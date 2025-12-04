const pool = require("../db");
import sendMailAWS from "../methods/sendMailAWS";
import updateRecord from "../utils/updateRecord";

async function sendForms() {
  //console.log("sendForms job running...");

  const conn = await pool.getConnection();
  try {
    const [processingRows] = await conn.query(
      `SELECT id FROM email_queue WHERE status = 'PROCESSING' LIMIT 1`
    );
    if (processingRows.length > 0) {
      console.log("A PROCESSING record exists. Skipping...");
      return;
    }

    const [pendingRows] = await conn.query(
      `SELECT * FROM email_queue WHERE status = 'PENDING' AND retry_count < 3 ORDER BY created_at ASC LIMIT 1`
    );
    if (pendingRows.length === 0) {
      console.log("No PENDING records found.");
      return;
    }
    //console.log("pendingRows:", pendingRows);
    const {
      id,
      subject,
      payload,
      recipient_email: recipientEmail,
      recipient_id: recipientId,
      template_name: templateName,
      retry_count,
      user_stakeholder_form_id,
      user_stakeholder_form_logs_id,
    } = pendingRows[0];

    try {
      const markProcessingResult = await updateRecord(
        "email_queue",
        { status: "PROCESSING" },
        { id: id }
      );

      if (markProcessingResult.affectedRows === 0) {
        console.warn("Failed to mark record as PROCESSING:", id);
        return;
      }

      let new_payload;
      try {
        new_payload =
          typeof payload === "string" ? JSON.parse(payload) : payload;
      } catch (e) {
        console.error("Failed to parse payload JSON:", payload);
        throw new Error("Invalid JSON format in payload");
      }
      //console.log("new_payload", new_payload);

      const isSent = await sendMailAWS(
        subject,
        new_payload, //assumes payload was saved as JSON string
        recipientEmail,
        templateName
      );

      if (isSent) {
        console.log("Email sent successfully:", recipientEmail);
        //
        // Update the other table with sent_ids and tot_email_sent---starts
        const updateCountAndIds = await conn.query(
          `
            UPDATE user_stakeholder_form_logs
            SET 
              sent_ids = TRIM(BOTH ',' FROM CONCAT_WS(',', sent_ids, ?)),
              tot_email_sent = tot_email_sent + 1
            WHERE id = ?
            `,
          [recipientId, user_stakeholder_form_logs_id]
        );
        if (updateCountAndIds.affectedRows === 0) {
          console.warn(
            "Failed to updateCountAndIds:",
            user_stakeholder_form_id
          );
          return;
        }
        // Update the other table with sent_ids and tot_email_sent---ends
        //
        //delete the email queue records ----starts
        const [deleteResult] = await pool.query(
          `DELETE FROM \`email_queue\` WHERE id = ?`,
          [id]
        );
        if (deleteResult.affectedRows === 0) {
          console.warn("Delete failed for email_queue ID:", id);
        }
        //delete the email queue records ----starts
        //
      } else {
        console.warn("Email sending failed for:", recipientEmail);
        const newRetryCount = retry_count + 1;
        // Determine next status
        const nextStatus = newRetryCount >= 3 ? "FAILED" : "PENDING";
        // Update retry and status++++++++++++++++++++++++++starts
        try {
          const retryResult = await updateRecord(
            "email_queue",
            {
              retry_count: newRetryCount,
              status: nextStatus,
            },
            { id: id }
          );

          if (retryResult.affectedRows === 0) {
            console.warn("Retry update failed for email_queue ID:", id);
          }
        } catch (retryErr) {
          console.error("Error updating retry and status:", retryErr.message);
        }
        // Update retry and status++++++++++++++++++++++++++ends
        //
        // If retry count has hit max (3), store in failed_ids-----starts
        if (newRetryCount >= 3) {
          try {
            const updateCountAndIds = await conn.query(
              `
            UPDATE user_stakeholder_form_logs
            SET 
              failed_ids = TRIM(BOTH ',' FROM CONCAT_WS(',', failed_ids, ?)),
              tot_email_failed = tot_email_failed + 1
            WHERE id = ?
            `,
              [recipientId, user_stakeholder_form_logs_id]
            );
            if (updateCountAndIds.affectedRows === 0) {
              console.warn(
                "Failed to updateCountAndIds:",
                user_stakeholder_form_id
              );
              return;
            }
          } catch (updateCountIdsErr) {
            console.error(
              "Error updateCountAndIds:",
              updateCountIdsErr.message
            );
          }
        }
        // If retry count has hit max (3), store in failed_ids-----ends
        //
      }
    } catch (err) {
      console.error("Error during email processing:", err.message);

      // Fallback: Always update retry_count in case of failure
      const newRetryCount = retry_count + 1;
      // Determine next status
      const nextStatus = newRetryCount >= 3 ? "FAILED" : "PENDING";
      // Update retry and status++++++++++++++++++++++++++starts
      try {
        const retryResult = await updateRecord(
          "email_queue",
          {
            retry_count: newRetryCount,
            status: nextStatus,
          },
          { id: id }
        );

        if (retryResult.affectedRows === 0) {
          console.warn("Retry update failed for email_queue ID:", id);
        }
      } catch (retryErr) {
        console.error("Error updating retry and status:", retryErr.message);
      }
      // Update retry and status++++++++++++++++++++++++++ends
      //
      // If retry count has hit max (3), store in failed_ids-----starts
      if (newRetryCount >= 3) {
        try {
          const updateCountAndIds = await conn.query(
            `
            UPDATE user_stakeholder_form_logs
            SET 
              failed_ids = TRIM(BOTH ',' FROM CONCAT_WS(',', failed_ids, ?)),
              tot_email_failed = tot_email_failed + 1
            WHERE id = ?
            `,
            [recipientId, user_stakeholder_form_logs_id]
          );
          if (updateCountAndIds.affectedRows === 0) {
            console.warn(
              "Failed to updateCountAndIds:",
              user_stakeholder_form_id
            );
            return;
          }
        } catch (updateCountIdsErr) {
          console.error("Error updateCountAndIds:", updateCountIdsErr.message);
        }
      }
      // If retry count has hit max (3), store in failed_ids-----ends
      //
    }
  } catch (outerError) {
    console.error("sendForms outer error:", outerError.message);
  } finally {
    if (conn) conn.release();
  }
}

module.exports = sendForms;
