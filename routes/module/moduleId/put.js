import Joi from "joi";
import updateRecord from "../../../utils/updateRecord";

const schema = Joi.object({
  name: Joi.string().required(),
  isActive: Joi.boolean().required(),
});

export default async (req, res, next) => {
  try {
    const { body, moduleId } = req;
    //console.log("PUT /moduleId - body:", body);
    //console.log("PUT /moduleId - moduleId:", moduleId);
    const data = await schema.validateAsync(body);
    const result = await updateRecord("module", data, {
      id: moduleId,
    });
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Module not found or nothing was updated" });
    }
    return res.json({
      message: "Module Updated Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in PUT /moduleId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
