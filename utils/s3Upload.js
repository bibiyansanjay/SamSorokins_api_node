// import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
// import dotenv from "dotenv";

// dotenv.config();
// console.log("process.env.AWS_ACCESS_KEY_ID", process.env.AWS_ACCESS_KEY_ID);
// console.log(
//   "process.env.AWS_SECRET_ACCESS_KEY",
//   process.env.AWS_SECRET_ACCESS_KEY
// );
// console.log("process.env.AWS_BUCKET", process.env.AWS_BUCKET);
// console.log("process.env.AWS_REGION", process.env.AWS_REGION);
// // const s3Client = new S3Client({
//   region: process.env.AWS_REGION,
//   credentials: {
//     accessKeyId: process.env.AWS_ACCESS_KEY_ID,
//     secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
//   },
// });

// export const uploadToS3 = async (fileBuffer, fileName, mimeType) => {
//   const folder = "profile-Images";
//   const finalKey = `${folder}/${Date.now()}_${fileName.replace(/\s+/g, "_")}`;
//   const bucketName = process.env.AWS_BUCKET;

//   const command = new PutObjectCommand({
//     Bucket: bucketName,
//     Key: finalKey,
//     Body: fileBuffer,
//     ContentType: mimeType,
//   });

//   await s3Client.send(command);

//   const s3Url = `https://${bucketName}.s3.${
//     process.env.AWS_REGION || "us-east-1"
//   }.amazonaws.com/${finalKey}`;
//   return { s3Url, finalKey };
// };

import AWS from "aws-sdk";

export const s3Upload = async (Bucket, Key, Body, ContentType) => {
  try {
    AWS.config.update({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION || "us-east-2",
    });

    const s3 = new AWS.S3();

    const params = {
      Bucket,
      Key,
      Body,
      ContentType,
    };

    const result = await s3.upload(params).promise();

    return result;
  } catch (error) {
    console.error("Error uploading to S3:", error);
    throw error;
  }
};
