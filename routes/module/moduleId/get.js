import getRecord from "../../../utils/getRecord";

export default async (req, res, next) => {
  try {
    const { moduleId } = req;
    // Fetch module data based on the provided conditions and options
    const whereConditions = { id: moduleId };
    const moduleData = await getRecord("module", whereConditions);
    if (!moduleData || moduleData.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No module data found",
        data: [],
      });
    }

    return res.status(200).json({
      success: true,
      message: "Module data fetched successfully",
      data: moduleData,
    });
  } catch (error) {
    console.error("GET /module - Error:", error);

    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching module data",
      error: error.message,
    });
  }
};
