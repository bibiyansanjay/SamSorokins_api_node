import updateRecord from "../../../utils/updateRecord";

export default async (req, res, next) => {
  try {
    const { moduleId } = req;
    const whereConditions = { id: moduleId };
    //console.log("delete /moduleId - moduleId:", moduleId);
    const now = new Date();
    const fieldsToUpdate = {
      isDeleted: true,
      deleted_at: now,
      isActive: false,
    };
    const result = await updateRecord(
      "module",
      fieldsToUpdate,
      whereConditions
    );
    //console.log("delete /moduleId - update result:", result);
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Module not found or nothing was updated" });
    }
    return res.json({
      message: "Module Deleted Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in delete /moduleId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
