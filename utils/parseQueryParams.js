export const parseQueryParams = (url) => {
  const baseUrl = "http://dummy.com"; // Needed for URL parsing
  const fullUrl = new URL(url, baseUrl);
  const queryParams = fullUrl.searchParams;

  const whereConditions = {};
  const options = {};

  // Handle pagination (DataGrid)
  const page = parseInt(queryParams.get("paginationModel[page]")) || 0;
  const pageSize = parseInt(queryParams.get("paginationModel[pageSize]")) || 10;
  options.limit = pageSize;
  options.offset = page * pageSize;

  // Sorting
  const sortField = queryParams.get("sortModel[0][field]");
  const sortOrder = queryParams.get("sortModel[0][sort]");
  if (sortField && sortOrder) {
    options.sort = {
      field: sortField,
      order: sortOrder.toUpperCase(),
    };
  }

  // Search functionality
  const searchValue = queryParams.get("searchValue")?.trim();
  const searchFields = queryParams
    .get("searchFields")
    ?.split(",")
    .map((f) => f.trim());

  if (searchValue && searchFields?.length > 0) {
    whereConditions.$or = searchFields.map((field) => ({
      [field]: {
        operator: "LIKE",
        value: `%${searchValue}%`,
      },
    }));
  }

  // Process all other query params
  for (const [key, value] of queryParams.entries()) {
    if (
      key.startsWith("paginationModel[") ||
      key.startsWith("sortModel[") ||
      key === "searchValue" ||
      key === "searchFields"
    ) {
      continue; // Already handled
    }

    // Advanced operators: field[operator]=value
    const match = key.match(/^(.+)\[(.+)\]$/);
    if (match) {
      const [, field, operatorKey] = match;
      const operatorMap = {
        gt: ">",
        lt: "<",
        gte: ">=",
        lte: "<=",
        ne: "!=",
        like: "LIKE",
        between: "between",
      };

      if (operatorKey === "between") {
        const [start, end] = value.split(",");
        whereConditions[field] = {
          operator: "between",
          value: [start, end],
        };
      } else {
        whereConditions[field] = {
          operator: operatorMap[operatorKey] || operatorKey,
          value: operatorKey === "like" ? `%${value}%` : value,
        };
      }
    } else {
      // Simple equality: role=2 or status=active
      whereConditions[key] = value;
    }
  }

  return { whereConditions, options };
};

// Query Purpose	URL Parameter Example
// Greater than	?score[gt]=80
// Less than	?score[lt]=30
// Greater than or equal to	?score[gte]=50
// Less than or equal to	?score[lte]=100
// Not equal	?status[ne]=inactive
// Between	?created_at[between]=2024-01-01,2024-12-31
// Like	?name[like]=manager
