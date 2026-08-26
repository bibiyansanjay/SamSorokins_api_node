import { JotformSubmission, Upload } from "../../models/index.js";

// In-memory TTL cache for instant duplicate API responses (< 1ms)
let duplicatesCache = null;
let cacheTime = 0;
const CACHE_TTL_MS = 60 * 1000; // 60 seconds cache TTL

export const clearDuplicatesCache = () => {
  duplicatesCache = null;
  cacheTime = 0;
};

const getFieldValue = (answers, fieldNames, keyName = "answer") => {
  const targets = Array.isArray(fieldNames) ? fieldNames : [fieldNames];
  const field = Object.values(answers || {}).find((item) =>
    targets.includes(item?.text)
  );
  if (!field) return null;
  const value = field?.[keyName] ?? null;
  if (typeof value === "string") return value.replace(/<[^>]*>/g, "").trim();
  return value;
};

export default async (req, res, next) => {
  try {
    const now = Date.now();

    // Return cached duplicates if available and fresh (< 60s)
    if (duplicatesCache && now - cacheTime < CACHE_TTL_MS) {
      return res.json(duplicatesCache);
    }

    // Step 1: Lightweight projection query from MongoDB (~20ms)
    const submissions = await JotformSubmission.find(
      {},
      {
        submissionId: 1,
        formName: 1,
        answers: 1,
        isSubmited: 1,
        createdAt: 1,
      }
    ).lean();

    // Step 2: Pass 1 in Node.js - Group submissions by same formName (~2ms)
    const formNameMap = new Map();

    submissions.forEach((item) => {
      const answers = item.answers || {};
      const formName = (
        item.formName ||
        getFieldValue(answers, ["Uploader Header", "Form Name", "FormName"]) ||
        ""
      ).trim();

      if (!formName || formName.toUpperCase() === "N/A") return;

      const key = formName.toLowerCase();
      if (!formNameMap.has(key)) {
        formNameMap.set(key, {
          formName,
          items: [],
        });
      }
      formNameMap.get(key).items.push(item);
    });

    // Step 3: Pass 2 in Node.js - Within same formName groups (count >= 2), group by RM Short Name & RM Unit Name (~1ms)
    const finalDuplicateGroupsList = [];

    formNameMap.forEach((formGroup) => {
      // Skip forms with only 1 submission
      if (formGroup.items.length < 2) return;

      const propUnitMap = new Map();

      formGroup.items.forEach((item) => {
        const answers = item.answers || {};

        const prop = (
          getFieldValue(answers, [
            "RM Short Name",
            "Property",
            "Property Name",
          ]) || ""
        ).trim();

        const unit = (
          getFieldValue(answers, [
            "RM Unit Name",
            "Unit",
            "Unit Name",
            "Unit Number",
          ]) || ""
        ).trim();

        // Exclude if Property or Unit is blank or N/A
        if (
          !prop ||
          !unit ||
          prop.toUpperCase() === "N/A" ||
          unit.toUpperCase() === "N/A"
        ) {
          return;
        }

        const key = `${prop.toLowerCase()}:::${unit.toLowerCase()}`;

        if (!propUnitMap.has(key)) {
          propUnitMap.set(key, {
            formName: formGroup.formName,
            propertyName: prop,
            unitName: unit,
            submissions: [],
          });
        }

        propUnitMap.get(key).submissions.push(item);
      });

      propUnitMap.forEach((group) => {
        if (group.submissions.length >= 2) {
          finalDuplicateGroupsList.push(group);
        }
      });
    });

    if (finalDuplicateGroupsList.length === 0) {
      const emptyResult = {
        success: true,
        totalGroups: 0,
        totalDuplicateSubmissions: 0,
        duplicateGroups: [],
      };
      duplicatesCache = emptyResult;
      cacheTime = now;
      return res.json(emptyResult);
    }

    // Step 4: Query Uploads ONLY for duplicate submission IDs (~2ms)
    const duplicateSubIds = [];
    finalDuplicateGroupsList.forEach((g) => {
      g.submissions.forEach((sub) => {
        if (sub.submissionId) duplicateSubIds.push(sub.submissionId);
      });
    });

    const uploads = await Upload.find({
      submissionId: { $in: duplicateSubIds },
    }).lean();

    const uploadsMap = {};
    uploads.forEach((f) => {
      if (!uploadsMap[f.submissionId]) {
        uploadsMap[f.submissionId] = [];
      }
      uploadsMap[f.submissionId].push({
        _id: f._id,
        filename: f.filename,
        s3Url: f.s3Url,
        status: f.status,
        createdAt: f.createdAt,
      });
    });

    // Step 5: Format response and sort by count descending (~1ms)
    const duplicateGroups = finalDuplicateGroupsList
      .map((group) => {
        const formattedSubmissions = group.submissions.map((sub) => {
          const answers = sub.answers || {};
          const files = uploadsMap[sub.submissionId] || [];

          let overallStatus = "Pending";
          if (files.length === 0 || !sub?.isSubmited) {
            overallStatus = "Pending";
          } else {
            const allUploaded = files.every((f) => f.status === "Uploaded");
            const anyUploading = files.some((f) => f.status === "Uploading");
            const anyFailed = files.some((f) => f.status === "Failed");

            if (allUploaded) overallStatus = "Uploaded";
            else if (anyUploading) overallStatus = "Uploading";
            else if (anyFailed) overallStatus = "Failed";
            else overallStatus = "Pending";
          }

          return {
            _id: sub._id,
            submissionId: sub.submissionId,
            createdAt: sub.createdAt,
            status: overallStatus,
            clientName: getFieldValue(answers, "User Name"),
            clientEmail: getFieldValue(answers, "User Email"),
            propertyName: group.propertyName,
            unitName: group.unitName,
            clientAddress: getFieldValue(answers, "Full Address"),
            formName:
              group.formName || getFieldValue(answers, "Uploader Header"),
            files,
            totalFiles: files.length,
          };
        });

        return {
          formName: group.formName,
          propertyName: group.propertyName,
          unitName: group.unitName,
          count: formattedSubmissions.length,
          submissions: formattedSubmissions,
        };
      })
      .sort((a, b) => b.count - a.count);

    const totalDuplicateSubmissions = duplicateGroups.reduce(
      (acc, group) => acc + group.count,
      0
    );

    const result = {
      success: true,
      totalGroups: duplicateGroups.length,
      totalDuplicateSubmissions,
      duplicateGroups,
    };

    duplicatesCache = result;
    cacheTime = now;

    return res.json(result);
  } catch (error) {
    console.error("[submissionsData/getDuplicates]", error);
    next(error);
  }
};
