'use server';

import { db, connectDb } from '@/lib/db';
import { Comment} from '@/lib/db/schema';
import { QueryOptions, buildCommentsQuery, buildStatsQueries, buildTimeSeriesQuery, buildRelatedCommentsQuery } from '../queryBuilder';
import { getCachedData } from '../cache';
import { unstable_cache } from 'next/cache';
import { cacheConfig } from '../cache-config';
import { StanceData, TimeSeriesResponse } from '@/lib/types/timeSeries';

// Response types
export interface CommentsPaginatedResponse {
  success: boolean;
  data?: Comment[];
  total?: number;
  error?: string;
}

export interface CommentsStatisticsResponse {
  success: boolean;
  stats?: {
    total: number;
    for: number;
    against: number;
    neutral: number;
  };
  error?: string;
}

/**
 * Parses URL search params into QueryOptions
 */
export async function parseUrlToQueryOptions(
  searchParams: URLSearchParams | Record<string, string>
): Promise<QueryOptions> {
  const options: QueryOptions = {
    filters: {},
    page: 1,
    pageSize: 10,
  };

  // Helper function to handle both URLSearchParams and plain objects
  const getParam = (key: string): string | null => {
    if (searchParams instanceof URLSearchParams) {
      return searchParams.get(key);
    } else if (typeof searchParams === 'object' && searchParams !== null) {
      return searchParams[key] || null;
    }
    return null;
  };

  // Helper function to get entries from either URLSearchParams or plain object
  const getEntries = (): [string, string][] => {
    if (searchParams instanceof URLSearchParams) {
      return Array.from(searchParams.entries());
    } else if (typeof searchParams === 'object' && searchParams !== null) {
      return Object.entries(searchParams);
    }
    return [];
  };

  // Parse search query
  const search = getParam('search');
  if (search) {
    options.search = search;
  }

  // Parse sorting
  const sort = getParam('sort');
  const sortDirection = getParam('sortDirection') as 'asc' | 'desc' | null;
  if (sort && sortDirection) {
    options.sort = {
      column: sort,
      direction: sortDirection,
    };
  }

  // Parse pagination
  const page = getParam('page');
  if (page) {
    options.page = parseInt(page, 10);
  }

  const pageSize = getParam('pageSize');
  if (pageSize) {
    options.pageSize = parseInt(pageSize, 10);
  }

  // Parse filters
  for (const [key, value] of getEntries()) {
    if (key.startsWith('filter_')) {
      const filterKey = key.replace('filter_', '');
      
      // Try to parse JSON for complex filters
      try {
        options.filters![filterKey] = JSON.parse(value);
      } catch {
        // If not valid JSON, use as string
        options.filters![filterKey] = value;
      }
    }
  }

  return options;
}

/**
 * Helper function to safely extract count value from DB result
 */
function extractCountValue(result: { rows?: Array<Record<string, unknown>> }): number {
  if (!result.rows || !Array.isArray(result.rows) || !result.rows[0]) return 0;
  
  const row = result.rows[0] as Record<string, unknown>;
  // Try to access the count property directly first
  if (typeof row.count === 'number') return row.count;
  if (typeof row.count === 'string') return parseInt(row.count, 10);
  
  // If that fails, get the first property (which should be the count)
  const firstKey = Object.keys(row)[0];
  if (!firstKey) return 0;
  
  const value = row[firstKey];
  if (typeof value === 'number') return value;
  if (typeof value === 'string') return parseInt(value, 10);
  
  return 0;
}

/**
 * Fetches paginated comments based on provided query options
 */
export async function getPaginatedComments(
  options: QueryOptions
): Promise<CommentsPaginatedResponse> {
  // Core function that fetches comments
  const fetchComments = async () => {
    try {
      const connection = await connectDb();
      if (!connection.success) {
        throw new Error("Failed to connect to database");
      }

      // Build queries for data and count
      const queryResult = await buildCommentsQuery(options);

      // Execute count query first
      const countResult = await db.execute(queryResult.countQuery);
      const total = extractCountValue(countResult);

      // Execute data query
      const result = await db.execute(queryResult.query);
      const data = result.rows as Comment[];

      return {
        success: true,
        data,
        total,
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error("Error fetching paginated comments:", errorMessage);
      return { 
        success: false, 
        error: `Failed to fetch comments: ${errorMessage}` 
      };
    }
  };

  // Check if we should skip caching
  const shouldSkipCache = process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment;
  
  if (shouldSkipCache) {
    return fetchComments();
  }

  // Use Next.js unstable_cache for production or when cache is enabled
  const getCachedComments = unstable_cache(
    fetchComments,
    [`comments-${JSON.stringify(options)}`], // Cache key
    {
      revalidate: 86400, // 24 hours, matching your page revalidation
      tags: ['comments']
    }
  );
  
  return getCachedComments();
}
// frontend/src/lib/actions/comments.ts
// Update the getStanceTimeSeries function

export async function getStanceTimeSeries(
  options: QueryOptions,
  dateField: 'postedDate' | 'receivedDate' = 'postedDate',
  includeDuplicates: boolean = true // New parameter
): Promise<TimeSeriesResponse> {
  const fetchTimeSeries = async () => {
    try {
      const connection = await connectDb();
      if (!connection.success) {
        throw new Error("Failed to connect to database");
      }

      // Build the time series query with duplicate filter
      const query = await buildTimeSeriesQuery(options, dateField, includeDuplicates);

      // Execute the query
      const result = await query;
      
      // Transform the flat results into the StanceData format
      const groupedByDate = new Map<string, StanceData>();
      
      for (const row of result as Array<{ date: string | Date | null; stance: string | null; count: number }>) {
        let dateObject: Date;

        if (row.date instanceof Date) {
          dateObject = row.date;
        } else if (typeof row.date === 'string') {
          dateObject = new Date(row.date);
          if (isNaN(dateObject.getTime())) {
            console.warn(`Skipping row due to unparsable date string: '${row.date}'`, row);
            continue;
          }
        } else if (row.date === null || typeof row.date === 'undefined') {
          console.warn("Skipping row with null or undefined date:", row);
          continue;
        } else {
          console.warn(`Skipping row with unexpected date type: ${typeof row.date}`, row);
          continue;
        }
        
        const dateStr = dateObject.toISOString().split('T')[0];
        
        if (!groupedByDate.has(dateStr)) {
          groupedByDate.set(dateStr, {
            date: dateStr,
            For: 0,
            Against: 0,
            'Neutral/Unclear': 0,
          });
        }
        
        const dayData = groupedByDate.get(dateStr)!;
        const stance = row.stance || 'Neutral/Unclear';
        
        if (stance === 'For' || stance === 'Against' || stance === 'Neutral/Unclear') {
          dayData[stance] = row.count;
        }
      }

      // Convert to array and sort by date
      const timeSeriesData = Array.from(groupedByDate.values())
        .sort((a, b) => a.date.localeCompare(b.date));

      return {
        success: true,
        data: timeSeriesData,
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error("Error fetching time series:", errorMessage);
      return { 
        success: false, 
        error: `Failed to fetch time series: ${errorMessage}` 
      };
    }
  };

  // Check if we should skip caching
  const shouldSkipCache = process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment;
  
  if (shouldSkipCache) {
    return fetchTimeSeries();
  }

  // Use Next.js unstable_cache for production
  const getCachedTimeSeries = unstable_cache(
    fetchTimeSeries,
    [`time-series-${dateField}-${includeDuplicates}-${JSON.stringify(options)}`], // Updated cache key
    {
      revalidate: 86400, // 24 hours
      tags: ['time-series', 'comments']
    }
  );
  
  return getCachedTimeSeries();
}


/**
 * Fetches statistics based on current filters
 */
export async function getCommentStatistics(
  options: QueryOptions
): Promise<CommentsStatisticsResponse> {
  // Create cache key based on options (without pagination)
  const filterOptions = { ...options };
  delete filterOptions.page;
  delete filterOptions.pageSize;
  
  // Core function that fetches statistics
  const fetchStatistics = async () => {
    try {
      const connection = await connectDb();
      if (!connection.success) {
        throw new Error("Failed to connect to database");
      }

      // Build all stats queries
      const queryResults = await buildStatsQueries(options);

      // Execute all queries
      const [totalResult, forResult, againstResult, neutralResult] = await Promise.all([
        db.execute(queryResults.totalQuery),
        db.execute(queryResults.forQuery),
        db.execute(queryResults.againstQuery),
        db.execute(queryResults.neutralQuery)
      ]);

      // Parse results
      const totalCount = extractCountValue(totalResult);
      const forCount = extractCountValue(forResult);
      const againstCount = extractCountValue(againstResult);
      const neutralCount = extractCountValue(neutralResult);

      return {
        success: true,
        stats: {
          total: totalCount,
          for: forCount,
          against: againstCount,
          neutral: neutralCount
        }
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error("Error fetching comment statistics:", errorMessage);
      return { 
        success: false, 
        error: `Failed to fetch statistics: ${errorMessage}` 
      };
    }
  };

  // Check if we should skip caching
  const shouldSkipCache = process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment;
  
  if (shouldSkipCache) {
    return fetchStatistics();
  }

  // Use Next.js unstable_cache for production or when cache is enabled
  const getCachedStats = unstable_cache(
    fetchStatistics,
    [`stats-${JSON.stringify(filterOptions)}`], // Cache key
    {
      revalidate: 86400, // 24 hours, matching your page revalidation
      tags: ['stats']
    }
  );
  
  return getCachedStats();
}

/**
 * Fetches statistics with duplicates removed based on current filters
 */
export async function getDedupedCommentStatistics(
  options: QueryOptions
): Promise<CommentsStatisticsResponse> {
  // Create cache key based on options (without pagination)
  const filterOptions = { ...options };
  delete filterOptions.page;
  delete filterOptions.pageSize;
  
  // Core function that fetches deduped statistics
  const fetchDedupedStatistics = async () => {
    try {
      const connection = await connectDb();
      if (!connection.success) {
        throw new Error("Failed to connect to database");
      }

      // Build all stats queries with deduplication
      const queryResults = await buildStatsQueries(options, false); // false = exclude duplicates

      // Execute all queries
      const [totalResult, forResult, againstResult, neutralResult] = await Promise.all([
        db.execute(queryResults.totalQuery),
        db.execute(queryResults.forQuery),
        db.execute(queryResults.againstQuery),
        db.execute(queryResults.neutralQuery)
      ]);

      // Parse results
      const totalCount = extractCountValue(totalResult);
      const forCount = extractCountValue(forResult);
      const againstCount = extractCountValue(againstResult);
      const neutralCount = extractCountValue(neutralResult);

      return {
        success: true,
        stats: {
          total: totalCount,
          for: forCount,
          against: againstCount,
          neutral: neutralCount
        }
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error("Error fetching deduped comment statistics:", errorMessage);
      return { 
        success: false, 
        error: `Failed to fetch deduped statistics: ${errorMessage}` 
      };
    }
  };

  // Check if we should skip caching
  const shouldSkipCache = process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment;
  
  if (shouldSkipCache) {
    return fetchDedupedStatistics();
  }

  // Use Next.js unstable_cache for production or when cache is enabled
  const getCachedDedupedStats = unstable_cache(
    fetchDedupedStatistics,
    [`deduped-stats-${JSON.stringify(filterOptions)}`], // Cache key
    {
      revalidate: 86400, // 24 hours, matching your page revalidation
      tags: ['deduped-stats']
    }
  );
  
  return getCachedDedupedStats();
}

/**
 * Fetches a single comment by ID
 */
export async function getCommentById(
  id: string
): Promise<{ success: boolean; data?: Comment; error?: string }> {
  const cacheKey = `comment-${id}`;

  return getCachedData(
    cacheKey,
    async () => {
      try {
        const connection = await connectDb();
        if (!connection.success) {
          throw new Error("Failed to connect to database");
        }

        const queryResult = await buildCommentsQuery({
          filters: { id }
        });

        const result = await db.execute(queryResult.query);
        const comments = result.rows as Comment[];

        if (comments.length === 0) {
          return { success: false, error: "Comment not found" };
        }

        return {
          success: true,
          data: comments[0]
        };
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        console.error("Error fetching comment by ID:", errorMessage);
        return { 
          success: false, 
          error: `Failed to fetch comment: ${errorMessage}` 
        };
      }
    }
  );
}


/**
 * Fetches related comments through the lookup table
 */
export async function getRelatedComments(
  lookupId: string
): Promise<{ success: boolean; data?: Comment[]; error?: string }> {
  const cacheKey = `related-comments-${lookupId}`;

  return getCachedData(
    cacheKey,
    async () => {
      try {
        const connection = await connectDb();
        if (!connection.success) {
          throw new Error("Failed to connect to database");
        }

        // Get the related comments through the lookup table
        const result = await buildRelatedCommentsQuery(lookupId);

        if (!result.relatedComments || result.relatedComments.length === 0) {
          return { success: true, data: [] };
        }

        return {
          success: true,
          data: result.relatedComments as Comment[]
        };
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        console.error("Error fetching related comments:", errorMessage);
        return { 
          success: false, 
          error: `Failed to fetch related comments: ${errorMessage}` 
        };
      }
    }
  );
}
