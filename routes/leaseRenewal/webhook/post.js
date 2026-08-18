import multer from "multer";

const upload = multer();

/**
 * @desc    Lease Renewal Webhook POST Handler
 * @route   POST /leaseRenewal/webhook
 * @access  Public
 */
export default async (req, res, next) => {
  try {
    let payload = req.body;

    console.log("==========================================");
    console.log("[WEBHOOK] Lease Renewal Received");
    console.log({ payload });

    return res.status(200).json({
      success: true,
      message: "Lease renewal webhook received successfully",
      timestamp: new Date().toISOString(),
      receivedData: req.body,
    });
  } catch (error) {
    console.error(
      "[WEBHOOK ERROR] Error handling lease renewal webhook:",
      error
    );
    return res.status(500).json({
      success: false,
      message: "Error processing lease renewal webhook",
      error: error.message,
    });
  }
};
