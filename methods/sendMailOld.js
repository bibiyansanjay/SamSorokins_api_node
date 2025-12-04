import nodemailer from "nodemailer";
import { google } from "googleapis";
const OAuth2 = google.auth.OAuth2;

const OAuth2_client = new OAuth2(
  process.env.CLIENT_ID,
  process.env.CLIENT_SECRET_KEY
);

OAuth2_client.setCredentials({ refresh_token: process.env.REFRESH_TOKEN });

const sendMailOld = async (
  subject,
  payload,
  recipient,
  templateName,
  cc,
  attachments
) => {
  try {
    //old methods---------------------------------------------------starts
    console.log("refreshToken:", process.env.REFRESH_TOKEN);
    const accessTokenResponse = await OAuth2_client.getAccessToken();
    console.log("accessTokenResponse:", accessTokenResponse);

    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        type: "OAuth2",
        user: process.env.GMAIL_EMAIL_USER,
        clientId: process.env.CLIENT_ID,
        clientSecret: process.env.CLIENT_SECRET_KEY,
        refreshToken: process.env.REFRESH_TOKEN,
        accessToken: accessTokenResponse.token,
      },
    });
    //old methods---------------------------------------------------ends
    //
    const result = await transporter.sendMail(mailOptions);
    //console.log("Email sent successfully:", result);
    transporter.close();
    return true;
  } catch (error) {
    console.error("Failed to send email:", error);
    throw error;
  }
};

export default sendMailOld;
