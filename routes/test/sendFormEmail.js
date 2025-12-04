import pool from "../../db";
import sendMailAWS from "../../methods/sendMailAWS";
import updateRecord from "../../utils/updateRecord";

export default async (req, res, next) => {
  try {
    const subcategoryFormId = 12;
    const now = new Date();
    //
    //step-0 update user_subcategory_form+++++++++++++++++++++++++++++++++++++++starts
    const whereConditionsSubcategoryFormStart = { id: subcategoryFormId };
    //console.log(" /subcategoryFormId - subcategoryFormId:", subcategoryFormId);

    const fieldsToUpdateInMainFormStart = {
      isProcessingEmails: true,
    };

    const result1 = await updateRecord(
      "user_subcategory_form",
      fieldsToUpdateInMainFormStart,
      whereConditionsSubcategoryFormStart
    );

    if (result1.affectedRows === 0) {
      console.log("Error updating user_subcategory_form for processing emails");
      return res
        .status(404)
        .json({ message: "Form not found or nothing was updated" });
    }

    //step-0 update user_subcategory_form+++++++++++++++++++++++++++++++++++++++ends

    // STEP 1: Fetch all forms via subcategory id, user_charity_links, user_global_send_options---------------starts
    const [forms] = await pool.query(
      `SELECT
          usf.*,
          usf.id AS user_form_id,
          ucl.*,
          ucl.id AS charity_link_id,
          gso.id AS global_send_option_id,
          gso.signatureName,
          gso.signatureTeam,
          gso.signatureTitle,
          gso.signatureCompany
          FROM \`user_stakeholder_form\` usf
          LEFT JOIN \`user_charity_links\` ucl ON usf.company = ucl.company
          LEFT JOIN \`user_global_send_options\` gso ON usf.company = gso.company
          WHERE usf.user_subcategory_form_id = ?`,
      [subcategoryFormId]
    );

    if (!forms.length) {
      return res
        .status(404)
        .json({ message: "No forms found for the provided ID" });
    }
    // STEP 1: Fetch all forms via subcategory id, user_charity_links, user_global_send_options-------------ends
    //
    //manage main value which are common to forms
    let formData = forms[0];
    //form-1--handle  current_wave, next_wave, next_wave_date, date - add frequency days to current date
    const frequency = formData.frequency;
    const automatic_reminder = formData.automatic_reminder;
    //
    // Format in NL date style: DD-MM-YYYY (ignoring time)
    async function formatDateToYMD(date) {
      const yyyy = date.getFullYear();
      const mm = String(date.getMonth() + 1).padStart(2, "0");
      const dd = String(date.getDate()).padStart(2, "0");
      return `${yyyy}-${mm}-${dd}`;
    }
    //
    let current_wave = 1;
    // console.log("frequency", frequency);
    // console.log("frequency", typeof frequency);
    //console.log("current_wave", current_wave);
    var next_wave = null;
    let next_wave_date = new Date(now);
    let last_distribution_date = new Date(now);
    switch (frequency) {
      case "daily":
        //console.log("daily");
        next_wave_date.setDate(now.getDate() + 1);
        next_wave = current_wave + 1;
        break;
      case "weekly":
        //console.log("weekly");
        next_wave_date.setDate(now.getDate() + 7);
        next_wave = current_wave + 1;
        break;
      case "bi-weekly":
        //console.log("bi-weekly");
        next_wave_date.setDate(now.getDate() + 14);
        next_wave = current_wave + 1;
        break;
      case "monthly":
        //console.log("monthly");
        next_wave_date.setMonth(now.getMonth() + 1);
        next_wave = current_wave + 1;
        break;
      case "quarterly":
        //console.log("quarterly");
        next_wave_date.setMonth(now.getMonth() + 3);
        next_wave = current_wave + 1;
        break;
      case "semi-annually":
        //console.log("semi-annually");
        next_wave_date.setMonth(now.getMonth() + 6);
        next_wave = current_wave + 1;
        break;
      case "annually":
        //console.log("annually");
        next_wave_date.setFullYear(now.getFullYear() + 1);
        next_wave = current_wave + 1;
        break;
      case "ad-hoc":
      default:
        //console.log("ad-hoc or default");
        next_wave = null;
        next_wave_date = null;
        break;
    }
    // console.log("next_wave", next_wave);
    // console.log("last_distribution_date", last_distribution_date);
    // console.log("next_wave_date", next_wave_date);
    //
    //calculate reminder_date-----------------------------------
    let reminder_date = null;

    if (automatic_reminder == 1) {
      const option = formData.reminder_limit; // e.g., "24h", "48h", "3d", "7d", "2w"
      let futureDate = new Date();
      switch (option) {
        case "24h":
          futureDate = new Date(now.getTime() + 24 * 60 * 60 * 1000);
          break;
        case "48h":
          futureDate = new Date(now.getTime() + 48 * 60 * 60 * 1000);
          break;
        case "3d":
          futureDate = new Date(now.getTime() + 3 * 24 * 60 * 60 * 1000);
          break;
        case "7d":
          futureDate = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
          break;
        case "2w":
          futureDate = new Date(now.getTime() + 14 * 24 * 60 * 60 * 1000);
          break;
        default:
          console.warn("Unknown reminder option:", option);
          futureDate = null;
      }

      if (futureDate) {
        // Format as YYYY-MM-DD
        reminder_date = futureDate.toISOString().split("T")[0];
      }
    }
    //console.log("reminder_date", reminder_date);
    //
    // STEP 2: Loop through each form---------starts
    for (const form of forms) {
      //console.log("form", form);
      //
      //form-2---------get client users--------------starts
      const stakeholder = form.stakeholder_id; // Example: single stakeholder value
      const segmentInput = form.segments; // Could be "24" or "24,52"

      const segmentArray = segmentInput
        .split(",")
        .map((val) => parseInt(val.trim()))
        .filter(Boolean); // Ensure numbers

      const placeholders = segmentArray.map(() => "?").join(",");

      const sql = `
                  SELECT *
                  FROM client_users
                  WHERE isActive = 1
                    AND isDeleted = 0
                    AND stakeholder = ?
                    AND segment IN (${placeholders})
                `;

      const [receipients] = await pool.query(sql, [
        stakeholder,
        ...segmentArray,
      ]);
      //form-2---------get client users--------------ends

      if (!receipients.length) {
        continue; // Skip to next form
      }

      const receipientsIds = receipients.map((r) => r.id).join(",");
      console.log(receipientsIds); // Output: "1,2,3"
      const jotform_id = form.jotform_id;
      let successIds = [];
      let failedIds = [];
      //form-3 loop receipients --------------------------------STARTS
      for (const receipient of receipients) {
        //if (receipient.id !== 1) continue; //used to test with single user
        const full_name = receipient.name || "";
        //console.log("full_name", full_name);
        //3.1--create JotForm URL
        const queryParams = new URLSearchParams({
          full_name: full_name || "",
          email: receipient.email || "",
          wave: current_wave,
          //form_id: form.user_form_id,
          company_id: form.company,
          form_type: form.form_type,
          frequency: form.frequency,
          respondent_id: receipient.id,
        });
        const jotform_url = `https://form.jotform.com/${jotform_id}?${queryParams.toString()}`;
        console.log("jotform_url", jotform_url);
        //3.2 create payload for email
        const payload = {
          logoUrl:
            "https://insightflowstorage.s3.eu-north-1.amazonaws.com/companyLogo/niftyailogo.png",
          subject: form.subject,
          //preheader: form.preheader,
          donation_amount: form.donation_amount,
          name: full_name?.trim().split(/\s+/)[0] || "",
          body: form.body,
          charities: [
            {
              name: form.charity_a_name,
              url: form.charity_a_url,
              key: "charityA",
            },
            {
              name: form.charity_b_name,
              url: form.charity_b_url,
              key: "charityB",
            },
            {
              name: form.charity_c_name,
              url: form.charity_c_url,
              key: "charityC",
            },
          ],
          formLink: jotform_url,
          selectedCharity: form.charity_a_name,
          signoff_style: form.signoff_style,
          signatureName: form.signatureName,
          signatureTeam: form.signatureTeam,
          signatureTitle: form.signatureTitle,
          signatureCompany: form.signatureCompany,
        };

        const subject = form.subject;
        const recipientEmail = receipient.email || "";
        //const recipientEmail = "bibiyansanjay@gmail.com";
        //const templateName = "template-sendForm";
        const templateName = "template-sendForm";
        console.log("payload", payload);
        // console.log("subject", subject);
        // console.log("recipient", recipientEmail);
        // console.log("templateName", templateName);
        //
        //3.3 send email
        const isSent = await sendMailAWS(
          subject,
          payload,
          recipientEmail,
          templateName
        );

        if (isSent) {
          successIds.push(receipient.id);
        } else {
          failedIds.push(receipient.id);
        }
        //
        //form-step2 send email %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%ENDS
        //
      }
      //form-3 loop receipients --------------------------------ENDS
      //
      // manage sent_ids and failed_ids---------------------
      // Prepare comma-separated strings
      const successIdsStr = successIds.join(",");
      const failedIdsStr = failedIds.join(",");

      // Save to DB or log
      // console.log("Success:", successIdsStr);
      // console.log("Failed:", failedIdsStr);
      // Form-4 Update the form record with jotform ID and URL-----------------------starts
      const fieldsToUpdateInEachForm = {};
      //
      if (form.form_type === "recurring") {
        fieldsToUpdateInEachForm["isOutstanding"] = true;
        fieldsToUpdateInEachForm["next_wave"] = next_wave;
        fieldsToUpdateInEachForm["next_wave_date"] = await formatDateToYMD(
          next_wave_date
        );
        fieldsToUpdateInEachForm["isComplete"] = false;
        fieldsToUpdateInEachForm["completed_at"] = "";
      } else if (form.form_type === "ad-hoc") {
        fieldsToUpdateInEachForm["isOutstanding"] = false;
        fieldsToUpdateInEachForm["isComplete"] = true;
        const now1 = new Date();
        fieldsToUpdateInEachForm["completed_at"] = now1
          .toISOString()
          .slice(0, 19)
          .replace("T", " ");
      }
      //
      fieldsToUpdateInEachForm["current_wave"] = current_wave;
      fieldsToUpdateInEachForm["last_distribution_date"] =
        await formatDateToYMD(last_distribution_date);
      fieldsToUpdateInEachForm["sent_ids"] = successIdsStr;
      fieldsToUpdateInEachForm["failed_ids"] = failedIdsStr;
      fieldsToUpdateInEachForm["tot_email_sent"] = successIds.length;
      fieldsToUpdateInEachForm["tot_email_failed"] = failedIds.length;
      fieldsToUpdateInEachForm["reminder_date"] = reminder_date;
      //fieldsToUpdateInEachForm["isProcessingEmails"] = false;
      //console.log("fieldsToUpdateInEachForm", fieldsToUpdateInEachForm);

      const whereConditionsForm = { id: form.user_form_id };
      const resultUpdateForm = await updateRecord(
        "user_stakeholder_form",
        fieldsToUpdateInEachForm,
        whereConditionsForm
      );

      // Form-4 Update the form record with jotform ID and URL-----------------------starts
    }
    //calc success no n fail no of emails sent

    // STEP 2: Loop through each form---------ends

    //step-3 update user_subcategory_form+++++++++++++++++++++++++++++++++++++++starts
    const whereConditionsSubcategoryForm = { id: subcategoryFormId };
    //console.log(" /subcategoryFormId - subcategoryFormId:", subcategoryFormId);

    const fieldsToUpdateInMainForm = {
      isEmailSent: true,
      email_sent_at: new Date(now),
      sendLater: false, // Reset sendLater after completion
      isProcessingEmails: false,
    };

    const result = await updateRecord(
      "user_subcategory_form",
      fieldsToUpdateInMainForm,
      whereConditionsSubcategoryForm
    );

    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Subcategory_Form not found or nothing was updated" });
    }

    //step-3 update user_subcategory_form+++++++++++++++++++++++++++++++++++++++ends
    return res.json({
      message: "Emails Sent Successfully",
      //affectedRows: result.affectedRows,
      // receipientslist: receipientslist,
      // formlist: formlist,
    });
  } catch (error) {
    //next(error);
    console.error(
      "Error in marking complete /subcategoryFormId:",
      error.message
    );
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
