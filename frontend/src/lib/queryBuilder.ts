// lib/queryBuilder.ts
'use server';

import { and, or, SQL, sql, eq } from 'drizzle-orm';
import { comments, stanceEnum, lookupTable } from './db/schema';
import { db } from './db';

export type FilterMode = 'exact' | 'includes' | 'at_least';
export type SortDirection = 'asc' | 'desc';
export type DateFilterMode = 'exact' | 'range' | 'before' | 'after';

export interface FilterValue {
  values: unknown[];
  mode: FilterMode;
}

export interface DateFilterValue {
  mode: DateFilterMode;
  startDate?: string;
  endDate?: string;
}

export interface SortOption {
  column: string;
  direction: SortDirection;
}

export interface QueryOptions {
  filters?: Record<string, unknown>;
  search?: string;
  searchFields?: string[];
  sort?: SortOption;
  page?: number;
  pageSize?: number;
}

/**
 * Gets the corresponding column from the comments table
 */
function getColumn(key: string): SQL | null {
  // Convert snake_case key (from URL) to camelCase (for Drizzle schema object)
  const camelCaseKey = key.replace(/_([a-z])/g, (g) => g[1].toUpperCase());

  if (camelCaseKey in comments) {
    const col = comments[camelCaseKey as keyof typeof comments];
    return sql`${col}`;
  }
  // Fallback to check the original key if no conversion was needed or if it was already camelCase
  if (key in comments) {
    const col = comments[key as keyof typeof comments];
    return sql`${col}`;
  }
  console.warn(`Column not found for key: ${key} (tried ${camelCaseKey})`);
  return null;
}

/**
 * Builds SQL condition for stance enum values
 */
function buildStanceCondition(column: SQL, value: string): SQL | null {
  switch (value) {
    case 'For':
      return sql`${column} = ${stanceEnum.enumValues[0]}`;
    case 'Against':
      return sql`${column} = ${stanceEnum.enumValues[1]}`;
    case 'Neutral/Unclear':
      return sql`${column} = ${stanceEnum.enumValues[2]}`;
    default:
      return null;
  }
}

/**
 * Builds conditions for stance filters
 */
function buildStanceFilterConditions(column: SQL, filterValue: FilterValue): SQL[] {
  const conditions: SQL[] = [];
  
  if (filterValue.mode === 'at_least') {
    // "Must include all" mode - add each condition individually (AND)
    filterValue.values.forEach(stanceValue => {
      const condition = buildStanceCondition(column, String(stanceValue));
      if (condition) conditions.push(condition);
    });
  } else if (filterValue.mode === 'exact' && filterValue.values.length === 1) {
    // Exact match for single value
    const condition = buildStanceCondition(column, String(filterValue.values[0]));
    if (condition) conditions.push(condition);
  } else {
    // "includes" mode - any match is sufficient (OR)
    const stanceConditions: SQL[] = [];
    filterValue.values.forEach(stanceValue => {
      const condition = buildStanceCondition(column, String(stanceValue));
      if (condition) stanceConditions.push(condition);
    });
    
    if (stanceConditions.length > 0) {
      conditions.push(sql`(${or(...stanceConditions)})`);
    }
  }
  
  return conditions;
}

/**
 * Builds SQL condition for commentCount ranges
 */
function buildCommentCountCondition(column: SQL, value: string): SQL | null {
  switch (value) {
    case '1':
      return sql`${column} = 1`;
    case '2-10':
      return sql`${column} >= 2 AND ${column} <= 10`;
    case '11-50':
      return sql`${column} >= 11 AND ${column} <= 50`;
    case '50+':
      return sql`${column} > 50`;
    default:
      // Try to parse as a number for direct values
      const num = parseInt(value, 10);
      if (!isNaN(num)) {
        return sql`${column} = ${num}`;
      }
      return null;
  }
}

/**
 * Builds conditions for commentCount filters
 */
function buildCommentCountFilterConditions(column: SQL, filterValue: FilterValue): SQL[] {
  const conditions: SQL[] = [];
  
  if (filterValue.mode === 'at_least') {
    // "Must include all" mode - doesn't make sense for ranges, treat as OR
    const countConditions: SQL[] = [];
    filterValue.values.forEach(countValue => {
      const condition = buildCommentCountCondition(column, String(countValue));
      if (condition) countConditions.push(condition);
    });
    
    if (countConditions.length > 0) {
      conditions.push(sql`(${or(...countConditions)})`);
    }
  } else if (filterValue.mode === 'exact' && filterValue.values.length === 1) {
    // Exact match for single value
    const condition = buildCommentCountCondition(column, String(filterValue.values[0]));
    if (condition) conditions.push(condition);
  } else {
    // "includes" mode - any match is sufficient (OR)
    const countConditions: SQL[] = [];
    filterValue.values.forEach(countValue => {
      const condition = buildCommentCountCondition(column, String(countValue));
      if (condition) countConditions.push(condition);
    });
    
    if (countConditions.length > 0) {
      conditions.push(sql`(${or(...countConditions)})`);
    }
  }
  
  return conditions;
}

/**
 * Builds conditions for theme filters
 */
function buildThemeFilterConditions(column: SQL, filterValue: FilterValue): SQL[] {
  const conditions: SQL[] = [];
  const themeValues = filterValue.values;
  
  if (themeValues.length === 0) return conditions;
  
  if (filterValue.mode === 'at_least') {
    // "Must Include At Least" - ALL themes must be present (AND)
    themeValues.forEach(theme => {
      conditions.push(sql`${column} ILIKE ${`%${theme}%`}`);
    });
  } else if (filterValue.mode === 'exact') {
    // "Exact Match" - exact string match
    const exactThemes = themeValues.join(',');
    conditions.push(sql`${column} = ${exactThemes}`);
  } else {
    // "Must Include Any" (includes mode) - ANY theme must be present (OR)
    const themeConditions: SQL[] = [];
    themeValues.forEach(theme => {
      themeConditions.push(sql`${column} ILIKE ${`%${theme}%`}`);
    });
    
    if (themeConditions.length > 0) {
      conditions.push(sql`(${or(...themeConditions)})`);
    }
  }
  
  return conditions;
}

/**
 * Builds conditions for date filters
 */
function buildDateFilterConditions(column: SQL, dateValue: DateFilterValue): SQL[] {
  const conditions: SQL[] = [];
  
  if (dateValue.mode === 'exact' && dateValue.startDate) {
    // For exact date, match the date portion only
    const date = new Date(dateValue.startDate);
    const nextDay = new Date(date);
    nextDay.setDate(nextDay.getDate() + 1);
    
    conditions.push(sql`${column} >= ${date.toISOString()} AND ${column} < ${nextDay.toISOString()}`);
  } else if (dateValue.mode === 'range' && dateValue.startDate && dateValue.endDate) {
    // For date range
    const startDate = new Date(dateValue.startDate);
    const endDate = new Date(dateValue.endDate);
    endDate.setHours(23, 59, 59, 999); // Include the entire end day
    
    conditions.push(sql`${column} >= ${startDate.toISOString()} AND ${column} <= ${endDate.toISOString()}`);
  } else if (dateValue.mode === 'before' && dateValue.endDate) {
    // Before a specific date
    const date = new Date(dateValue.endDate);
    conditions.push(sql`${column} < ${date.toISOString()}`);
  } else if (dateValue.mode === 'after' && dateValue.startDate) {
    // After a specific date
    const date = new Date(dateValue.startDate);
    date.setDate(date.getDate() + 1); // Exclusive of the date itself
    conditions.push(sql`${column} >= ${date.toISOString()}`);
  }
  
  return conditions;
}

/**
 * Builds conditions for text filters (with multiple values)
 */
function buildTextFilterConditions(column: SQL, values: string[]): SQL[] {
  const conditions: SQL[] = [];
  const textConditions: SQL[] = [];
  
  values.forEach(textValue => {
    textConditions.push(sql`${column} ILIKE ${`%${textValue}%`}`);
  });
  
  if (textConditions.length > 0) {
    conditions.push(sql`(${or(...textConditions)})`);
  }
  
  return conditions;
}

/**
 * Determines if a field should use text search (ILIKE) vs exact match
 */
function isTextSearchField(key: string): boolean {
  const textFields = ['comment', 'title', 'keyQuote', 'category', 'rationale', 'key_quote', 'submitterName', 'organization', 'city', 'state', 'country', 'documentType', 'commentOn', 'attachmentUrls', 'attachmentTitles'];
  return textFields.includes(key);
}

/**
 * Builds filter conditions for a single filter
 */
function buildSingleFilterCondition(key: string, value: unknown, column: SQL): SQL[] {
  const conditions: SQL[] = [];
  
  // Handle filter objects with values and mode
  if (
    value &&
    typeof value === 'object' &&
    'values' in value &&
    'mode' in value && // Ensure 'mode' exists for FilterValue
    Array.isArray((value as { values: unknown[] }).values) // Check if 'values' is an array
  ) {
    const filterValue = value as FilterValue; // This cast is now safer
    
    if (filterValue.values.length === 0) return conditions;
    
    // Special handling for stance
    if (key === 'stance') {
      return buildStanceFilterConditions(column, filterValue);
    }
    
    // Special handling for themes
    if (key === 'themes') {
      return buildThemeFilterConditions(column, filterValue);
    }
    
    // Special handling for commentCount/comment_count
    if (key === 'commentCount' || key === 'comment_count' || key === 'attachmentCount' || key === 'attachment_count') {
      return buildCommentCountFilterConditions(column, filterValue);
    }
    
    // General handling for other fields with mode
    if (filterValue.mode === 'at_least') {
      // For "must include all" mode - add each condition
      filterValue.values.forEach(val => {
        conditions.push(sql`${column} ILIKE ${`%${val}%`}`);
      });
    } else if (filterValue.mode === 'exact') {
      // For exact match mode
      const exactValue = filterValue.values.join(',');
      conditions.push(sql`${column} = ${exactValue}`);
    } else {
      // For "includes" mode - any match is sufficient
      const likeConditions: SQL[] = [];
      filterValue.values.forEach(val => {
        likeConditions.push(sql`${column} ILIKE ${`%${val}%`}`);
      });
      
      if (likeConditions.length > 0) {
        conditions.push(sql`(${or(...likeConditions)})`);
      }
    }
  }
  // Handle date filters
  else if (value && typeof value === 'object' && 'mode' in value && ('startDate' in value || 'endDate' in value)) {
    return buildDateFilterConditions(column, value as DateFilterValue);
  }
  // Handle array filters
  else if (Array.isArray(value)) {
    if (value.length === 0) return conditions;
    
    if (key === 'themes') {
      // For themes field (comma-separated string in database)
      const themeConditions: SQL[] = [];
      value.forEach(theme => {
        if (typeof theme === 'string') {
          themeConditions.push(sql`${column} ILIKE ${`%${theme}%`}`);
        }
      });
      
      if (themeConditions.length > 0) {
        conditions.push(sql`(${or(...themeConditions)})`);
      }
    } else if (key === 'stance') {
      // Special handling for stance enum in array
      const stanceConditions: SQL[] = [];
      value.forEach(stanceValue => {
        const condition = buildStanceCondition(column, String(stanceValue));
        if (condition) stanceConditions.push(condition);
      });
      
      if (stanceConditions.length > 0) {
        conditions.push(sql`(${or(...stanceConditions)})`);
      }
    } else if (key === 'commentCount' || key === 'comment_count' || key === 'attachmentCount' || key === 'attachment_count') {
      // Special handling for count ranges in array
      const countConditions: SQL[] = [];
      value.forEach(countValue => {
        const condition = buildCommentCountCondition(column, String(countValue));
        if (condition) countConditions.push(condition);
      });
      
      if (countConditions.length > 0) {
        conditions.push(sql`(${or(...countConditions)})`);
      }
    } else if (isTextSearchField(key)) {
      // For text fields, use ILIKE with OR for partial matches
      return buildTextFilterConditions(column, value.map(String));
    } else {
      // For regular array filters, use IN clause
      conditions.push(sql`${column} IN (${value})`);
    }
  }
  // Handle simple value filters
  else {
    if (key === 'stance') {
      const condition = buildStanceCondition(column, String(value));
      if (condition) conditions.push(condition);
    } else if (key === 'commentCount' || key === 'comment_count' || key === 'attachmentCount' || key === 'attachment_count') {
      // Special handling for count ranges
      const condition = buildCommentCountCondition(column, String(value));
      if (condition) conditions.push(condition);
    } else if (isTextSearchField(key)) {
      // Use ILIKE for text fields to search for partial matches
      conditions.push(sql`${column} ILIKE ${`%${value}%`}`);
    } else {
      // Use exact match for non-text fields
      conditions.push(sql`${column} = ${value}`);
    }
  }
  
  return conditions;
}

/**
 * Builds filter conditions for the query
 */
function buildFilterConditions(filters?: Record<string, unknown>): SQL[] {
  const conditions: SQL[] = [];
  
  if (!filters) return conditions;
  
  // Process each filter
  for (const [key, value] of Object.entries(filters)) {
    // Skip empty values
    if (value === undefined || value === null || value === '') {
      continue;
    }

    // Get the column from the comments table
    const column = getColumn(key);
    if (!column) {
      console.warn(`Column not found for filter key: ${key}`);
      continue;
    }

    // Build conditions for this filter
    const filterConditions = buildSingleFilterCondition(key, value, column);
    conditions.push(...filterConditions);
  }
  
  return conditions;
}

/**
 * Builds search conditions for the query
 */
function buildSearchConditions(search?: string, searchFields?: string[]): SQL[] {
  if (!search || search.trim() === '') {
    return [];
  }
  
  const searchValue = search.toLowerCase().trim();
  const searchConditions: SQL[] = [];
  
  // Determine which fields to search
  const fieldsToSearch = searchFields && searchFields.length > 0 
    ? searchFields 
    : ['comment', 'title', 'keyQuote', 'key_quote', 'themes', 'rationale', 'category', 'submitterName', 'organization', 'documentType', 'commentOn', 'attachmentUrls', 'attachmentTitles'];
  
  // Build search conditions for each field
  fieldsToSearch.forEach(field => {
    const column = getColumn(field);
    if (column) {
      searchConditions.push(sql`${column} ILIKE ${`%${searchValue}%`}`);
    }
  });
  
  // Return combined OR condition if we have any search conditions
  if (searchConditions.length > 0) {
    return [sql`(${or(...searchConditions)})`];
  }
  
  return [];
}

/**
 * Builds a query for the comments table based on provided filters and options
 */
export async function buildCommentsQuery(options: QueryOptions) {
  // Build filter conditions
  const filterConditions = buildFilterConditions(options.filters);
  
  // Build search conditions
  const searchConditions = buildSearchConditions(options.search, options.searchFields);
  
  // Combine all conditions
  const allConditions = [...filterConditions, ...searchConditions];
  
  // Create base query
  const baseQuery = db.select().from(comments);
  
  // Debug log in development
  if (process.env.NODE_ENV === 'development') {
    console.log('Building query with all comment fields, including commentCount');
  }
  
  // Create dynamic query for adding conditions
  const query = baseQuery.$dynamic();
  
  // Add where clause if there are conditions
  if (allConditions.length > 0) {
    query.where(and(...allConditions));
  }
  
  // Add sorting
  if (options.sort) {
    const { column, direction } = options.sort;
    const sortColumn = getColumn(column);
    
    if (sortColumn) {
      if (direction === 'asc') {
        query.orderBy(sql`${sortColumn} asc`);
      } else {
        query.orderBy(sql`${sortColumn} desc`);
      }
    }
  } else {
    // Default sorting
    query.orderBy(sql`${comments.createdAt} desc`);
  }
  
  // Add pagination
  if (options.page !== undefined && options.pageSize !== undefined) {
    const offset = (options.page - 1) * options.pageSize;
    query.limit(options.pageSize).offset(offset);
  }
  
  // Create count query
  const countBaseQuery = db.select({ count: sql`count(*)` }).from(comments);
  const countQuery = countBaseQuery.$dynamic();
  
  // Add where clause to count query if there are conditions
  if (allConditions.length > 0) {
    countQuery.where(and(...allConditions));
  }
  
  // Log queries for debugging in development
  if (process.env.NODE_ENV === 'development') {
    console.log('Main Query:', query.toSQL());
    console.log('Count Query:', countQuery.toSQL());
  }
  
  return { query, countQuery };
}

/**
 * Builds queries for statistics based on filters
 */
export async function buildStatsQueries(options: QueryOptions, includeDuplicates: boolean = true) {
  // Build filter conditions
  const filterConditions = buildFilterConditions(options.filters);
  
  // Build search conditions
  const searchConditions = buildSearchConditions(options.search, options.searchFields);
  
  // Combine all conditions
  const allConditions = [...filterConditions, ...searchConditions];
  
  // Add condition to exclude duplicates if requested
  if (!includeDuplicates) {
    // Only include the first comment from each lookup group
    allConditions.push(
      sql`${comments.id} IN (
        SELECT DISTINCT ON (${lookupTable.lookupId}) ${comments.id}
        FROM ${comments}
        LEFT JOIN ${lookupTable} ON ${comments.lookupId} = ${lookupTable.lookupId}
        ORDER BY ${lookupTable.lookupId}, ${comments.id}
      )`
    );
  }
  
  // Create base count query
  const totalBaseQuery = db.select({ count: sql`count(*)` }).from(comments);
  const totalQuery = totalBaseQuery.$dynamic();
  
  // Add conditions to total query
  if (allConditions.length > 0) {
    totalQuery.where(and(...allConditions));
  }
  
  // Get stance column
  const stanceColumn = comments.stance;
  
  // Create stance queries
  const forBaseQuery = db.select({ count: sql`count(*)` }).from(comments);
  const againstBaseQuery = db.select({ count: sql`count(*)` }).from(comments);
  const neutralBaseQuery = db.select({ count: sql`count(*)` }).from(comments);
  
  const forQuery = forBaseQuery.$dynamic();
  const againstQuery = againstBaseQuery.$dynamic();
  const neutralQuery = neutralBaseQuery.$dynamic();
  
  // Define stance conditions
  const forCondition = sql`${stanceColumn} = ${stanceEnum.enumValues[0]}`;
  const againstCondition = sql`${stanceColumn} = ${stanceEnum.enumValues[1]}`;
  const neutralCondition = sql`${stanceColumn} = ${stanceEnum.enumValues[2]}`;
  
  // Add conditions based on whether allConditions exist
  if (allConditions.length > 0) {
    forQuery.where(and(...[...allConditions, forCondition]));
    againstQuery.where(and(...[...allConditions, againstCondition]));
    neutralQuery.where(and(...[...allConditions, neutralCondition]));
  } else {
    forQuery.where(forCondition);
    againstQuery.where(againstCondition);
    neutralQuery.where(neutralCondition);
  }
  
  return {
    totalQuery,
    forQuery,
    againstQuery,
    neutralQuery
  };
}

/**
 * Build a query to fetch related comments through the lookup table
 */
export async function buildRelatedCommentsQuery(lookupId: string) {
  // First get the lookup table entry
  const lookupEntry = await db
    .select()
    .from(lookupTable)
    .where(eq(lookupTable.lookupId, lookupId))
    .limit(1);
  
  if (!lookupEntry.length) {
    return { relatedComments: [] };
  }
  
  const commentIds = lookupEntry[0].commentIds;
  
  // Fetch all comments with those IDs
  const relatedComments = await db
    .select()
    .from(comments)
    .where(sql`${comments.id} = ANY(${commentIds})`);
  
  return { relatedComments };
}

/**
 * Build time series query with duplicate handling through lookup table
 */
export async function buildTimeSeriesQuery(
  options: QueryOptions,
  dateField: 'postedDate' | 'receivedDate' = 'postedDate',
  includeDuplicates: boolean = true
) {
  // Build filter conditions
  const filterConditions = buildFilterConditions(options.filters);
  const searchConditions = buildSearchConditions(options.search, options.searchFields);
  const allConditions = [...filterConditions, ...searchConditions];

  // Add condition to only include primary comments (first in each lookup group) if excluding duplicates
  if (!includeDuplicates) {
    // We need to only include comments that are the first in their lookup group
    // This requires a subquery or window function
    allConditions.push(
      sql`${comments.id} IN (
        SELECT DISTINCT ON (${lookupTable.lookupId}) ${comments.id}
        FROM ${comments}
        LEFT JOIN ${lookupTable} ON ${comments.lookupId} = ${lookupTable.lookupId}
        ORDER BY ${lookupTable.lookupId}, ${comments.id}
      )`
    );
  }

  // Create the date truncation for grouping by day
  const dateTrunc = sql`DATE_TRUNC('day', ${comments[dateField]})`;
  
  // Build the aggregation query
  const baseQuery = db
    .select({
      date: dateTrunc.as('date'),
      stance: comments.stance,
      count: sql<number>`COUNT(*)`.mapWith(Number),
    })
    .from(comments)
    .$dynamic();

  // Add WHERE conditions if any
  if (allConditions.length > 0) {
    baseQuery.where(and(...allConditions));
  }

  // Group by date and stance
  baseQuery
    .groupBy(dateTrunc, comments.stance)
    .orderBy(sql`date ASC`);

  return baseQuery;
}

// Export helper functions for testing
export { 
  buildFilterConditions, 
  buildSearchConditions, 
  buildStanceCondition,
  isTextSearchField 
};