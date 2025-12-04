import Joi from "joi";
import updateRecord from "../../../utils/updateRecord";

const schema = Joi.object({
  page: Joi.number().integer().required(),
  title: Joi.string().max(255).required(),
  description: Joi.string().allow(null, "").optional(),
  video_url: Joi.string().uri().max(500).required(),
  category: Joi.number().integer().allow(null).required(),
  display_order: Joi.number().integer().allow(null).required(),
  language_code: Joi.string().max(10).default("en"),
  duration_seconds: Joi.number().integer().min(0).allow(null).optional(),
  thumbnail_url: Joi.string().uri().max(500).allow(null, "").optional(),
  isActive: Joi.boolean().required(),
});

export default async (req, res, next) => {
  try {
    const userRole = req.user.role;
    //console.log("userRole:", userRole);
    if (userRole != "1" && userRole != "6") {
      return res.status(500).json({
        success: false,
        message: "Un-Authrorized Access",
      });
    } else {
      const { body, videoId } = req;
      //console.log("PUT /videoId - body:", body);
      //console.log("PUT /videoId - videoId:", videoId);
      const data = await schema.validateAsync(body);
      const result = await updateRecord("videos", data, { id: videoId });
      if (result.affectedRows === 0) {
        return res
          .status(404)
          .json({ message: "Video not found or nothing was updated" });
      }
      return res.json({
        message: "Video Updated Successfully",
        affectedRows: result.affectedRows,
      });
    }
  } catch (error) {
    //next(error);
    console.error("Error in PUT /videoId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
