// import "server-only";
import nodemailer from "nodemailer";
import ejs from "ejs";
import path from "path";

const sendMail = async (
  subject,
  payload,
  recipient,
  templateName,
  cc,
  attachments
) => {
  try {
    //GMAIL-------------------------------starts working,

    // const transporter = nodemailer.createTransport({
    //   host: "smtp.gmail.com",
    //   port: 465,
    //   secure: true,
    //   auth: {
    //     user: process.env.GMAIL_EMAIL_USER,
    //     pass: process.env.GMAIL_APP_PASSWORD,
    //   },
    // });
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.GMAIL_EMAIL_USER?.trim(),
        pass: process.env.GMAIL_APP_PASSWORD?.trim(),
      },
    });

    const htmlContent = await ejs.renderFile(
      path.join(process.cwd(), "templates", `${templateName}.ejs`),
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
    console.log("Email sent successfully:", result);
    transporter.close();
    return true;
  } catch (error) {
    console.error("Failed to send email:", error);
    throw error;
  }
};

export default sendMail;
