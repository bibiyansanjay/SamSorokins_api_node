import getRecord from "../../../utils/getRecord";

export default async (req, res, next) => {
  try {
    const { roleId } = req;
    // Fetch role data based on the provided conditions and options
    const whereConditions = { id: roleId };
    const roleData = await getRecord("role", whereConditions);
    if (!roleData || roleData.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No role data found",
        data: [],
      });
    }

    return res.status(200).json({
      success: true,
      message: "Role data fetched successfully",
      data: roleData,
    });
  } catch (error) {
    console.error("GET /role - Error:", error);

    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching role data",
      error: error.message,
    });
  }
};
