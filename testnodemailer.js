const nodemailer = require("nodemailer");
require("dotenv").config();

const transporter = nodemailer.createTransport({
  host: "email-smtp.eu-north-1.amazonaws.com",
  port: 587,
  secure: false,
  auth: {
    user: process.env.AWS_SES_SMTP_USER1,
    pass: process.env.AWS_SES_SMTP_PASSWORD1,
  },
});

transporter.verify(function (error, success) {
  if (error) {
    console.log("Connection Failed: ", error);
  } else {
    console.log("SMTP connection successful ✅");
  }
});
