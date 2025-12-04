import updateRecord from "../../../utils/updateRecord";

export default async (req, res, next) => {
  try {
    const { userId } = req;
    const whereConditions = { id: userId };
    const fieldsToUpdate = {
      isDeleted: false,
      deleted_at: null,
      isActive: false,
    };
    const result = await updateRecord("user", fieldsToUpdate, whereConditions);
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "User not found or nothing was updated" });
    }
    return res.json({
      message: "User Restored successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in restore /userId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
