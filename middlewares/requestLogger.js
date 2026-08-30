import { RequestLog, User } from "../models/index.js";

/**
 * Extract and clean client IP address from request headers or socket.
 */
const getCleanIP = (req) => {
  const forwarded = req.headers["x-forwarded-for"];
  const realIp = req.headers["x-real-ip"];
  const cfIp = req.headers["cf-connecting-ip"];

  let rawIp = "";
  if (forwarded) {
    rawIp = String(forwarded).split(",")[0].trim();
  } else if (realIp) {
    rawIp = String(realIp).trim();
  } else if (cfIp) {
    rawIp = String(cfIp).trim();
  } else if (req.ip) {
    rawIp = req.ip;
  } else if (req.socket?.remoteAddress) {
    rawIp = req.socket.remoteAddress;
  } else if (req.connection?.remoteAddress) {
    rawIp = req.connection.remoteAddress;
  }

  if (!rawIp) return "Unknown IP";

  // Clean IPv6-mapped IPv4 address (e.g. ::ffff:192.168.1.1 -> 192.168.1.1)
  let clean = rawIp.replace(/^::ffff:/i, "").trim();

  // Strip port if formatted as IPv4:port
  if (
    clean.includes(":") &&
    !clean.includes("::") &&
    clean.split(":").length === 2
  ) {
    clean = clean.split(":")[0];
  }

  return clean || "Unknown IP";
};

/**
 * Helper to derive a clean human-readable request name from method and route path.
 */
const getRequestName = (method, urlPath) => {
  const path = urlPath.split("?")[0].toLowerCase();
  const m = method.toUpperCase();

  if (path === "/auth" && m === "POST") return "Login Request";
  if (path === "/auth/verifyotp" && m === "POST") return "Verify OTP";
  if (path === "/auth/sendotp" && m === "POST") return "Send OTP";
  if (path === "/auth/resendotp" && m === "POST") return "Resend OTP";
  if (path.startsWith("/forgotpassword")) return "Forgot Password";
  if (path.startsWith("/registeruser")) return "Register User";
  if (path.startsWith("/submissionsdata")) {
    if (path.includes("/duplicates")) return "Get Duplicate Submissions";
    if (m === "GET") return "Get Submissions";
    if (m === "DELETE") return "Delete Submission";
    if (m === "POST") return "Create Submission";
  }
  if (path.startsWith("/dashboard")) return "Get Dashboard Data";
  if (path.startsWith("/requestlogs") || path.startsWith("/logs"))
    return "Get Request Logs";
  if (path.startsWith("/users")) return `${m} Users`;
  if (path.startsWith("/keys")) return `${m} Keys`;
  if (path.startsWith("/rentmanager")) return `${m} Rent Manager`;
  if (path.startsWith("/leaserenewal")) return `${m} Lease Renewal`;
  if (path.startsWith("/files")) return `${m} Files`;

  return `${m} ${path}`;
};

const requestLogger = async (req, res, next) => {
  const urlPath = (req.originalUrl || req.url || "")
    .split("?")[0]
    .toLowerCase();

  // Skip OPTIONS preflight, favicon, internal dropdown options, webhooks, or log viewer APIs
  if (
    req.method === "OPTIONS" ||
    urlPath === "/favicon.ico" ||
    urlPath.startsWith("/requestlogs") ||
    urlPath.startsWith("/logs") ||
    urlPath.startsWith("/webhook") ||
    urlPath.startsWith("/test") ||
    urlPath === "/submissionsdata/options"
  ) {
    return next();
  }

  const startTime = Date.now();

  res.on("finish", async () => {
    try {
      const responseTime = Date.now() - startTime;
      const clientIP = getCleanIP(req);

      let email = req.user?.email || req.body?.email || req.query?.email || "";
      let userId = req.user?._id || req.user?.id || null;
      let role = req.user?.role || "";
      let isSuperAdmin = req.user?.isSuperAdmin || false;

      // If userId is missing but we have an email (e.g. login / verify OTP), try to look up user
      if (!userId && email) {
        try {
          const foundUser = await User.findOne({
            email: String(email).trim().toLowerCase(),
            isDeleted: false,
          }).select("_id email role isSuperAdmin");
          if (foundUser) {
            userId = foundUser._id;
            if (!email) email = foundUser.email;
            if (foundUser.role) role = foundUser.role;
            if (foundUser.isSuperAdmin) isSuperAdmin = foundUser.isSuperAdmin;
          }
        } catch (e) {
          // ignore lookup errors
        }
      }

      const strUserId = userId ? String(userId) : "";
      const normalizedRole = String(role).trim().toLowerCase();

      // Skip log creation for developer user ID (69bd177472681b58e9c964b9) or Super Admin role
      if (
        strUserId === "69bd177472681b58e9c964b9" ||
        // isSuperAdmin ||
        normalizedRole === "super admin" ||
        normalizedRole === "superadmin"
      ) {
        return;
      }

      const requestName = getRequestName(
        req.method,
        req.originalUrl || req.url
      );

      await RequestLog.create({
        requestName,
        userId: userId || null,
        email: email ? String(email).trim() : "",
        ipAddress: clientIP,
        method: req.method,
        url: req.originalUrl || req.url,
        statusCode: res.statusCode,
        responseTime,
        userAgent: req.headers["user-agent"] || "",
      });
    } catch (err) {
      console.error("Failed to save request log:", err?.message || err);
    }
  });

  next();
};

export default requestLogger;
