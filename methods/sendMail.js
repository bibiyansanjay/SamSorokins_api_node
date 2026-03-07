import nodemailer from "nodemailer";
import { google } from "googleapis";
import ejs from "ejs";
import path from "path";
const OAuth2 = google.auth.OAuth2;

const OAuth2_client = new OAuth2(
  process.env.CLIENT_ID,
  process.env.CLIENT_SECRET_KEY
);

OAuth2_client.setCredentials({ refresh_token: process.env.REFRESH_TOKEN });

/**
 * @method sendMail
 * @memberof module:Methods
 * @description Function to send mail.
 */

const sendMail = async (
  subject,
  payload,
  recipient,
  templateName,
  cc,
  attachments
) => {
  try {
    // const accessTokenResponse = await OAuth2_client.getAccessToken();
    // const transporter = nodemailer.createTransport({
    //   host: "mail.smtp2go.com", // or smtp.office365.com
    //   port: 2525,
    //   secure: false, // true for 465, false for other ports
    //   auth: {
    //     user: process.env.SMTP_EMAIL_USER, // your email address
    //     pass: process.env.SMTP_EMAIL_PASSWORD, // your app password
    //   },
    // });
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        type: "OAuth2",
        user: process.env.EMAIL_USER,
        clientId: process.env.CLIENT_ID,
        clientSecret: process.env.CLIENT_SECRET_KEY,
        refreshToken: process.env.REFRESH_TOKEN,
        // accessToken: accessTokenResponse.token,
      },
    });

    const htmlContent = await ejs.renderFile(
      path.join(__dirname, `../templates/${templateName}.ejs`),
      payload
    );

    const mailOptions = {
      from: process.env.EMAIL_USER,
      to: recipient,
      cc: cc,
      subject: subject,
      html: htmlContent,
      attachments,
    };

    const result = await transporter.sendMail(mailOptions);
    console.log("Email sent successfully:", result);
    transporter.close();
  } catch (error) {
    console.error("Failed to send email:", error);
    throw error;
  }
};

export default sendMail;
