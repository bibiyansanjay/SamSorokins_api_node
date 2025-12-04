import updateRecord from "../../../utils/updateRecord";

export default async (req, res, next) => {
  try {
    const { userId } = req;
    const whereConditions = { id: userId };
    //console.log("delete /userId - userId:", userId);
    const now = new Date();
    const fieldsToUpdate = {
      isDeleted: true,
      deleted_at: now,
      isActive: false,
    };
    const result = await updateRecord("user", fieldsToUpdate, whereConditions);
    //console.log("delete /userId - update result:", result);
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "User not found or nothing was updated" });
    }
    return res.json({
      message: "User Deleted Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in delete /userId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
