import { Key } from "../../models";

export const getKeysData = async (req, res, next) => {
  try {
    const { search, status, page, pageSize, limit: queryLimit } = req.query;

    const baseQuery = { isDeleted: { $ne: true } };

    // 1. Apply Search Filter
    if (search && typeof search === "string" && search.trim()) {
      const s = search.trim().replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const regex = new RegExp(s, "i");

      baseQuery.$or = [
        { rfId: regex },
        { rfid: regex },
        { frId: regex },
        { vendor: regex },
        { property: regex },
        { unit: regex },
        { phoneNumber: regex },
        { email: regex },
        { repairsEmail: regex },
        { serviceIssue: regex },
        { serviceRequest: regex },
        { fullAddress: regex },
        { whoWillPickUp: regex },
        { whoHasIt: regex },
        { status: regex },
        { userType: regex },
        { keysNeeded: regex },
        { lostReason: regex },
        { byWhen: regex },
        { pickUpDateTime: regex },
        { pickerPhoneNumber: regex },
        { pickerEmail: regex },
      ];
    }

    // 2. Compute statusCounts for active keys matching current search query
    const allKeysForCounts = await Key.find(baseQuery).select("status");

    const statusCounts = {
      all: allKeysForCounts.length,
      requested: 0,
      checkedOut: 0,
      permanently: 0,
      toBeReturned: 0,
      checkedIn: 0,
      lost: 0,
    };

    allKeysForCounts.forEach((k) => {
      const st = (k.status || "").toLowerCase().trim();
      if (st.includes("requested")) {
        statusCounts.requested++;
      } else if (st.includes("permanently")) {
        statusCounts.permanently++;
      } else if (st.includes("checked out")) {
        statusCounts.checkedOut++;
      } else if (st.includes("returned") || st.includes("outstanding")) {
        statusCounts.toBeReturned++;
      } else if (st.includes("checked in")) {
        statusCounts.checkedIn++;
      } else if (st.includes("lost")) {
        statusCounts.lost++;
      }
    });

    // 3. Apply Status Filter to final query
    const query = { ...baseQuery };

    if (status && typeof status === "string" && status !== "All") {
      const fStatus = status.toLowerCase().trim();

      if (fStatus === "requested") {
        query.status = { $regex: "requested", $options: "i" };
      } else if (fStatus === "checked out" || fStatus === "key checked out") {
        query.$and = (query.$and || []).concat([
          { status: { $regex: "checked out", $options: "i" } },
          { status: { $not: /permanently/i } },
        ]);
      } else if (fStatus === "checked out permanently") {
        query.status = { $regex: "permanently", $options: "i" };
      } else if (fStatus === "to be returned" || fStatus === "outstanding") {
        query.status = { $regex: "returned|outstanding", $options: "i" };
      } else if (fStatus === "checked in") {
        query.status = { $regex: "checked in", $options: "i" };
      } else if (fStatus === "lost") {
        query.status = { $regex: "lost", $options: "i" };
      } else {
        query.status = status;
      }
    }

    // 4. Count total documents matching current search + status query
    const total = await Key.countDocuments(query);

    // 5. Server-side Pagination
    let keysQuery = Key.find(query).sort({ createdAt: -1 });

    const limitVal = pageSize !== undefined ? pageSize : queryLimit;
    const pageVal = page;

    if (limitVal !== undefined || pageVal !== undefined) {
      const limitNum = Math.max(1, parseInt(limitVal, 10) || 12);
      const pageIndex = Math.max(0, parseInt(pageVal, 10) || 0);
      const skipNum = pageIndex * limitNum;
      keysQuery = keysQuery.skip(skipNum).limit(limitNum);
    }

    const keys = await keysQuery;

    // 6. Fetch unique properties
    const properties = await Key.distinct("property");

    return res.json({
      keys,
      total,
      statusCounts,
      properties,
    });
  } catch (error) {
    next(error);
  }
};
