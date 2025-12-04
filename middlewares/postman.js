// middleware/postmanFlag.js
export const postman = (req, res, next) => {
  /*   console.log("Before cleanup:");
  console.log("originalUrl:", req.originalUrl);
  console.log("baseUrl:", req.baseUrl);
  console.log("path:", req.path);
  console.log("url:", req.url);
  console.log("Params:", req.params);
  console.log("Query:", req.query); */

  // Parse full URL from originalUrl
  const full = new URL(req.originalUrl, "http://local.test");
  const params = full.searchParams;

  const hasPostmanADMIN =
    (params.get("postman") || req.params?.postman || "")
      .toString()
      .toLowerCase() === "admin";
  const hasPostmanSUPERADMIN =
    (params.get("postman") || req.params?.postman || "")
      .toString()
      .toLowerCase() === "superadmin";
  const hasPostmanCLIENT =
    (params.get("postman") || req.params?.postman || "")
      .toString()
      .toLowerCase() === "client";
  if (hasPostmanADMIN || hasPostmanSUPERADMIN || hasPostmanCLIENT) {
    // 1) Remove from search params
    params.delete("postman");

    // 2) Remove from route params (if present)
    if (req.params && "postman" in req.params) delete req.params.postman;

    // 3) Build cleaned full (original) URL
    const cleanedSearch = params.toString();
    const cleanedOriginalUrl =
      full.pathname + (cleanedSearch ? `?${cleanedSearch}` : "");

    // 4) Build router-relative req.url:
    //    strip the mount path (req.baseUrl) from pathname
    const base = req.baseUrl || "";
    const relPath = full.pathname.startsWith(base)
      ? full.pathname.slice(base.length) || "/"
      : req.path || "/";
    const cleanedReqUrl = relPath + (cleanedSearch ? `?${cleanedSearch}` : "");

    // 5) Force-override originalUrl (hacky but you asked for it)
    Object.defineProperty(req, "originalUrl", {
      value: cleanedOriginalUrl,
      writable: true,
      configurable: true,
      enumerable: true,
    });

    // 6) Replace router-relative url for downstream
    req.url = cleanedReqUrl;

    // 7) Clear Express caches so it re-parses from the new URLs
    req._parsedUrl = undefined;
    req._parsedOriginalUrl = undefined;

    // 8) Freeze req.query to the cleaned params (prevents re-hydrating postman)
    const cleanedQueryObj = {};
    for (const [k, v] of params.entries()) {
      // URLSearchParams folds arrays; if you expect arrays, handle append logic
      cleanedQueryObj[k] = v;
    }
    Object.defineProperty(req, "query", {
      value: cleanedQueryObj,
      writable: true,
      configurable: true,
      enumerable: true,
    });
    //console.log("Postman param detected and removed everywhere.");

    if (hasPostmanADMIN) {
      req.user = { id: 1, role: "1" }; //admin  role
    }
    if (hasPostmanSUPERADMIN) {
      req.user = { id: 6, role: "6" }; //super admin  role
    }
    if (hasPostmanCLIENT) {
      req.user = { id: 2, role: "2" }; //parent company role
    }

    /*     console.log("After cleanup:");
    console.log("originalUrl:", req.originalUrl);
    console.log("baseUrl:", req.baseUrl);
    console.log("path:", req.path);
    console.log("url:", req.url);
    console.log("Params:", req.params);
    console.log("Query:", req.query);
    console.log("req.user:", req.user); */
  }
  next();
};
