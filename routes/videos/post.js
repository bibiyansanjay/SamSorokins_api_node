import Joi from "joi";
import createRecord from "../../utils/createRecord";

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
    //other than admin/super-admin insert company
    if (userRole != "1" && userRole != "6") {
      return res.status(500).json({
        success: false,
        message: "Un-Authrorized Access",
      });
    } else {
      const { body } = req;
      //console.log("post body:", body);

      const data = await schema.validateAsync(body);
      const result = await createRecord("videos", data, {});
      if (result.affectedRows === 0) {
        console.error("Error in Video creation");
        return res.status(404).json({ message: "Video creation failed" });
      }
      return res.json({
        message: "Video Created Successfully",
        affectedRows: result.affectedRows,
      });
    }
  } catch (error) {
    console.log("Error adding Video", error);
    next(error);
  }
};
