import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

export default async (Bucket, Key, buffer, mimetype) => {
  try {
    const s3Client = new S3Client({
      region: process.env.AWS_REGION || "eu-north-1",
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });

    const s3Params = {
      Bucket,
      Key,
      Body: buffer,
      ContentType: mimetype,
    };

    const command = new PutObjectCommand(s3Params);
    const uploadResult = await s3Client.send(command);
    //console.log(uploadResult);
    // Return a consistent structure like v2 did
    return {
      Bucket,
      Key,
      ETag: uploadResult.ETag,
      Location: `https://${Bucket}.s3.${
        process.env.AWS_REGION || "eu-north-1"
      }.amazonaws.com/${Key}`,
    };
  } catch (error) {
    console.error("Error uploading to S3:", error);
    throw new Error("Failed to upload file to S3");
  }
};

/*import AWS from "aws-sdk";

export default async (Bucket, Key, buffer, mimetype) => {
  try {
    // Configure AWS SDK
    AWS.config.update({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION || "eu-north-1", // Ensure the region is flexible
    });

    const s3 = new AWS.S3();

    // Upload PDF to S3
    const s3Params = {
      Bucket: Bucket,
      Key: Key, // ✅ key must be a string
      Body: buffer,
      ContentType: mimetype,
    };

    // Upload the file to S3 and return the result
    const uploadResult = await s3.upload(s3Params).promise();
    //console.log(uploadResult);
    return uploadResult;
  } catch (error) {
    console.error("Error uploading to S3:", error);
    throw new Error("Failed to upload file to S3");
  }
};*/
