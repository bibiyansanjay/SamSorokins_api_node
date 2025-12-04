import nodemailer from "nodemailer";
import ejs from "ejs";
import path from "path";

const sendMailAWS = async (
  subject,
  payload,
  recipient,
  templateName,
  cc,
  attachments
) => {
  try {
    // console.log("AWS_SES_HOST:", process.env.AWS_SES_HOST);
    // console.log("AWS_SES_SMTP_USERNAME:", process.env.AWS_SES_SMTP_USERNAME);
    // console.log("AWS_SES_SMTP_PASSWORD:", process.env.AWS_SES_SMTP_PASSWORD);
    const transporter = nodemailer.createTransport({
      host: process.env.AWS_SES_HOST,
      port: 587,
      secure: false,
      auth: {
        user: process.env.AWS_SES_SMTP_USERNAME,
        pass: process.env.AWS_SES_SMTP_PASSWORD,
      },
    });

    const htmlContent = await ejs.renderFile(
      path.join(__dirname, `../templates/${templateName}.ejs`),
      payload
    );

    const mailOptions = {
      from: `"NiftyAI" <${process.env.SES_VERIFIED_FROM_EMAIL}>`,
      to: recipient,
      cc: cc,
      subject: subject,
      html: htmlContent,
      attachments,
    };
    const result = await transporter.sendMail(mailOptions);
    console.log("Email sent successfully:", result);
    transporter.close();
    return true;
  } catch (error) {
    console.error("Failed to send email:", error);
    throw error;
  }
};

export default sendMailAWS;
