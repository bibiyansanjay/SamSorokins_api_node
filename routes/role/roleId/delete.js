import updateRecord from "../../../utils/updateRecord";

export default async (req, res, next) => {
  try {
    const { roleId } = req;
    const whereConditions = { id: roleId };
    //console.log("delete /roleId - roleId:", roleId);
    const now = new Date();
    const fieldsToUpdate = {
      isDeleted: true,
      deleted_at: now,
      isActive: false,
    };
    const result = await updateRecord("role", fieldsToUpdate, whereConditions);
    //console.log("delete /roleId - update result:", result);
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Role not found or nothing was updated" });
    }
    return res.json({
      message: "Role Deleted Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in delete /roleId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
