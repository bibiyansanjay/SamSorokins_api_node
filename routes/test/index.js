import { Router } from "express";
const router = Router();
import sendFormEmail from "./sendFormEmail";
import pool from "../../db";
import { parseQueryParams } from "../../utils/parseQueryParams";
import sendMailAWS from "../../methods/sendMailAWS";
import avgrag from "./avgrag";

router.get("/", (req, res) => {
  try {
    res.send("Testing done Successfully.");
  } catch (error) {
    console.log("error", error);
    next(error);
  }
});
router.get("/sendFormEmail", sendFormEmail); //get all draft category forms

router.get("/avgrag", avgrag);

router.get("/webhook", async (req, res) => {
  try {
    const webhookURL = `https://4mk5ud65bfx6yk4irznrkt4uoq0egukd123.lambda-url.eu-north-1.on.aws/`;
    const formEncodedBody = `webhookURL=${encodeURIComponent(webhookURL)}`;

    const jotformID = "252076400409450";

    const webhookRes = await fetch(
      `https://eu-api.jotform.com/form/${jotformID}/webhooks?apiKey=${process.env.JOTFORM_API_KEY}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: formEncodedBody,
      }
    );

    const webhookData = await webhookRes.json();
    console.log(
      "Webhook creation response for form ID:",
      jotformID,
      webhookData
    );

    // Check if your target webhook URL exists in any of the values
    const existingWebhooks = Object.values(webhookData?.content || {});
    const webhookExists = existingWebhooks.includes(webhookURL);

    if (!webhookExists) {
      console.error("Failed to create webhook for form:", jotformID);
      console.error("Response:", webhookData);
      await pool.query(
        `INSERT INTO user_form_webhook_creation_failed (form) VALUES (?)`,
        [form.id]
      );
    } else {
      console.log("Webhook created or already exists for form:", jotformID);
    }

    res.send("Webhook test done successfully.");
  } catch (error) {
    console.log("error", error);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
});

router.get("/baseurl", (req, res) => {
  try {
    const protocol = req.protocol;
    const host = req.get("host");

    console.log("protocol", protocol);
    console.log("host", host);

    let baseurl;

    if (host === "localhost:5000") {
      baseurl = process.env.NODE_LOCAL_URL_NGROK; // e.g., http://localhost:5000 (defined in .env)
      //console.log("📦 Using NODE_LOCAL_URL_NGROK from env:", baseurl);
    } else {
      baseurl = `${protocol}://${host}`;
      //console.log("🌐 Using dynamic base URL:", baseurl);
    }

    const webhookURL = `${baseurl}/jotform/webhook`;
    console.log("✅ Webhook URL:", webhookURL);

    res.send({ message: "Testing done Successfully", webhookURL });
  } catch (error) {
    console.log("error", error);
    next(error);
  }
});
router.get("/email", async (req, res) => {
  try {
    const subject = "Test Email";
    const recipientEmail = "chhabrarohit472@gmail.com";
    const templateName = "template-sendForm";
    const payload = {
      logoUrl:
        "https://insightflowstorage.s3.eu-north-1.amazonaws.com/companyLogo/niftyailogo.png",
      subject: "Test Email",
      //preheader: "This is a test email preheader",
      donation_amount: "100",
      name: "Rohit Chhabra",
      body: "This is a test email body",
      charities: [
        {
          name: "charity_a_name",
          url: "www.charity-a.com",
          key: "charityA",
        },
        {
          name: "charity_b_name",
          url: "www.charity-b.com",
          key: "charityB",
        },
        {
          name: "charity_c_name",
          url: "www.charity-c.com",
          key: "charityC",
        },
      ],
      formLink: "www.charity-c.com",
      selectedCharity: "charity_a_name",
      signoff_style: "signoff_style",
      signatureName: "signatureName",
      signatureTeam: "signatureTeam",
      signatureTitle: "signatureTitle",
      signatureCompany: "signatureCompany",
    };
    console.log("payload", payload);
    const isSent = await sendMailAWS(
      subject,
      payload,
      recipientEmail,
      templateName
    );
    res.send({
      message: "Testing done Successfully",
      isSent: isSent,
    });
  } catch (error) {
    console.log("error", error);
    throw error;
  }
});
router.get("/response/qids", async (req, res) => {
  try {
    //res.send("Testing done Successfully.");
    const [rows] = await pool.query(
      `SELECT fdc.form_id, fdc.question_id, q.question_type, qt.name, COUNT(fdc.surrogate_key) as tot FROM form_data_company2 fdc LEFT JOIN question q ON q.id=fdc.question_id LEFT JOIN question_type qt ON qt.id=q.question_type GROUP BY fdc.question_id ORDER BY fdc.wave_id ASC`
    );
    //console.log("segRows", segRows);
    return res.status(200).json({
      success: true,
      rows: rows,
    });
  } catch (error) {
    console.log("error", error);
    next(error);
  }
});

router.get("/questions/crossreferencing", async (req, res) => {
  try {
    //id 1 handle------------------------------------------------------------------
    const sql1 = `
    UPDATE question q1
    JOIN question q2 
      ON q1.upload_r_q_id1 = q2.upload_q_id
    SET q1.reinforcement_q_id1 = q2.id
    WHERE q1.upload_r_q_id1 IS NOT NULL
      AND q1.upload_r_q_id1 != 'None'
      AND q1.reinforcement_q_id1 IS NULL;
  `;
    //res.send("Testing done Successfully.");
    const [result1] = await pool.query(sql1);
    //id 1 handle------------------------------------------------------------------
    //
    //id 2 handle------------------------------------------------------------------
    const sql2 = `
    UPDATE question q1
    JOIN question q2 
      ON q1.upload_r_q_id2 = q2.upload_q_id
    SET q1.reinforcement_q_id2 = q2.id
    WHERE q1.upload_r_q_id2 IS NOT NULL
      AND q1.upload_r_q_id2 != 'None'
      AND q1.upload_r_q_id2 != ''
      AND q1.reinforcement_q_id2 IS NULL;
  `;
    //res.send("Testing done Successfully.");
    const [result2] = await pool.query(sql2);
    //id 2 handle------------------------------------------------------------------
    console.log("handle1 Rows affected:", result1.affectedRows);
    console.log("handle2 Rows affected:", result2.affectedRows);
    return res.status(200).json({
      success: true,
      rows1: result1.affectedRows,
      rows2: result2.affectedRows,
    });
  } catch (error) {
    console.log("error", error);
    next(error);
  }
});
export default router;
