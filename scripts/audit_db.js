import "../db/config.js";
import db from "../db/index.js";
import { Upload } from "../models/index.js";

const run = async () => {
    console.log("Starting DB audit...");
    const timeout = setTimeout(() => {
        console.error("Timeout: Could not connect to DB or finish audit in 5s");
        process.exit(1);
    }, 5000);

    try {
        await db();
        const count = await Upload.countDocuments({});
        const uploads = await Upload.find({});
        
        console.log(`Total Uploads in DB: ${count}`);
        const summary = uploads.reduce((acc, u) => {
            acc[u.status] = (acc[u.status] || 0) + 1;
            return acc;
        }, {});
        console.log("Summary by status:", summary);
        
        // Check for missing submissionId
        const missingSub = uploads.filter(u => !u.submissionId || u.submissionId === 'unknown');
        console.log(`Uploads with missing/unknown submissionId: ${missingSub.length}`);
        
        clearTimeout(timeout);
        process.exit(0);
    } catch (err) {
        console.error("Audit failed:", err);
        process.exit(1);
    }
};

run().catch(err => {
    console.error(err);
    process.exit(1);
});
