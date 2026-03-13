/**
 * @method /users/userId/profile-img PUT
 * @memberof module:Routes.users
 * @description Function to handle PUT requests to update profile Image.
 */

import Joi from "joi";
import { User } from "../../../../models";
// import { uploadToS3 } from "../../../../../utils/s3Upload";
import { s3Upload } from "../../../../utils/s3Upload";

const schema = Joi.object({
  profileImg: Joi.string().allow("").allow(null),
  signature: Joi.string().allow("").allow(null),
});

export default async (req, res, next) => {
  try {
    const { body, userId } = req;
    const data = await schema.validateAsync(body);

    const file = req.file;
    if (!file) {
      return res.status(404).json({ message: "No file uploaded" });
    }

    // ✅ Validate image
    if (!file.mimetype.startsWith("image/")) {
      return res.status(404).json({ message: "Only image files are allowed" });
    }

    const bucket = process.env.AWS_BUCKET;

    const key = `profile-images/${Date.now()}_${file.originalname}`;

    const uploadResult = await s3Upload(
      bucket,
      key,
      file.buffer,
      file.mimetype
    );

    // const { s3Url } = await uploadToS3(
    //   file.buffer,
    //   file.originalname,
    //   file.mimetype
    // );

    const user = await User.findOneAndUpdate(
      { _id: userId },
      { profileImg: uploadResult?.Location, signature: data?.signature },
      { new: true }
    );

    if (!user) {
      return res.status(404).json({ message: "User Not Found" });
    }

    return res.json({
      message: "Profile Image Updated Successfully",
      url: uploadResult?.Location,
    });
  } catch (error) {
    next(error);
  }
};
