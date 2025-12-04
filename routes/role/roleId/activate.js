import Joi from "joi";
import updateRecord from "../../../utils/updateRecord";

export default async (req, res, next) => {
  try {
    const { roleId } = req;
    const whereConditions = { id: roleId };
    //console.log("activate /roleId - roleId:", roleId);
    const fieldsToUpdate = {
      isActive: true,
    };
    const result = await updateRecord("role", fieldsToUpdate, whereConditions);
    //console.log("activate /roleId - update result:", result);
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Role not found or nothing was updated" });
    }
    return res.json({
      message: "Role Activated Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in activate /roleId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
