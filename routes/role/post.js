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

    const userRole = req.user.role;
    const userId = req.user.id;
    const parentId = req.user.parent_id;

    const data = await schema.validateAsync(body);

    //other than admin/super-admin insert company
    if (userRole != "1" && userRole != "6") {
      data["company"] = parentId ? parentId : userId;
    }

    //for all user roles add created by from logged in person
    data["created_by"] = userId;

    const result = await createRecord("role", data, {});
    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Role creation failed" });
    }
    return res.json({
      message: "Role Created Successfully",
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
