import nodemailer from "nodemailer";
import ejs from "ejs";
import path from "path";

const sendMailGmail = async (
  subject,
  payload,
  recipient,
  templateName,
  cc,
  attachments
) => {
  try {
    //GMAIL-------------------------------starts working
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.GMAIL_EMAIL_USER, //gmail user
        pass: process.env.GMAIL_APP_PASSWORD, // <- Your app password here
      },
    });

    const htmlContent = await ejs.renderFile(
      path.join(__dirname, `../templates/${templateName}.ejs`),
      payload
    );

    const mailOptions = {
      from: process.env.GMAIL_EMAIL_USER,
      to: recipient,
      cc: cc,
      subject: subject,
      html: htmlContent,
      attachments,
    };
    //GMAIL-------------------------------ends working
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

export default sendMailGmail;
