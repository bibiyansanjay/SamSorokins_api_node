import updateRecord from "../../../utils/updateRecord";

export default async (req, res, next) => {
  try {
    const userRole = req.user.role;
    //console.log("userRole:", userRole);
    if (userRole != "1" && userRole != "6") {
      return res.status(500).json({
        success: false,
        message: "Un-Authrorized Access",
      });
    } else {
      const { videoId } = req;
      const whereConditions = { id: videoId };
      //console.log("delete /videoId - videoId:", videoId);
      const fieldsToUpdate = {
        isDeleted: true,
        isActive: false,
      };
      const result = await updateRecord(
        "videos",
        fieldsToUpdate,
        whereConditions
      );
      //console.log("delete /videoId - update result:", result);
      if (result.affectedRows === 0) {
        return res
          .status(404)
          .json({ message: "Video not found or nothing was updated" });
      }
      return res.json({
        message: "Video Deleted Successfully",
        affectedRows: result.affectedRows,
      });
    }
  } catch (error) {
    //next(error);
    console.error("Error in delete /videoId:", error.message);
    return res
      .status(500)
      .json({ message: "Internal Server Error", error: error.message });
  }
};
