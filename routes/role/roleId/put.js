import Joi from "joi";
import updateRecord from "../../../utils/updateRecord";

const schema = Joi.object({
  name: Joi.string().required(),
  isActive: Joi.boolean().required(),
});

export default async (req, res, next) => {
  try {
    const { body, roleId } = req;
    //console.log("PUT /roleId - body:", body);
    //console.log("PUT /roleId - roleId:", roleId);
    const data = await schema.validateAsync(body);
    const result = await updateRecord("role", data, { id: roleId });
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Role not found or nothing was updated" });
    }
    return res.json({
      message: "Role Updated Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in PUT /roleId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
