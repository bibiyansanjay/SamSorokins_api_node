import updateRecord from "../../../utils/updateRecord";

export default async (req, res, next) => {
  try {
    const { moduleId } = req;
    const whereConditions = { id: moduleId };
    //console.log("Activate /moduleId - moduleId:", moduleId);
    const fieldsToUpdate = {
      isActive: true,
    };
    const result = await updateRecord(
      "module",
      fieldsToUpdate,
      whereConditions
    );
    //console.log("Activate /moduleId - update result:", result);
    if (result.affectedRows === 0) {
      return res
        .status(404)
        .json({ message: "Module not found or nothing was updated" });
    }
    return res.json({
      message: "Module Activated Successfully",
      affectedRows: result.affectedRows,
    });
  } catch (error) {
    //next(error);
    console.error("Error in Activate /moduleId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
