import Joi from "joi";
import createRecord from "../../utils/createRecord";

const schema = Joi.object({
  name: Joi.string().required(),
  isActive: Joi.boolean().required(),
});

export default async (req, res, next) => {
  try {
    const { body } = req;
    //console.log("post body:", body);
    const data = await schema.validateAsync(body);
    data["value"] = data.name;
    const result = await createRecord("module", data, {});
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Module not found or nothing was updated" });
    }
    return res.json({
      message: "Module Created Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in post:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
