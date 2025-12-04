import updateRecord from "../../../utils/updateRecord";

export default async (req, res, next) => {
  try {
    const { roleId } = req;
    const whereConditions = { id: roleId };
    //console.log("restore /roleId - roleId:", roleId);
    const now = new Date();
    const fieldsToUpdate = {
      isDeleted: false,
      deleted_at: null,
      isActive: false,
    };
    const result = await updateRecord("role", fieldsToUpdate, whereConditions);
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Role not found or nothing was updated" });
    }
    return res.json({
      message: "Role Restored Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in restore /roleId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
