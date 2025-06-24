// src/lib/actions/index.ts
'use server';

// Explicitly import and re-export Server Actions and types from ./comments
import {
  parseUrlToQueryOptions,
  getPaginatedComments,
  getCommentStatistics,
  getCommentById
} from './comments';

export type {
  CommentsPaginatedResponse,
  CommentsStatisticsResponse
} from './comments';

export {
  parseUrlToQueryOptions,
  getPaginatedComments,
  getCommentStatistics,
  getCommentById
};

// Re-export existing functions from the old actions.ts
import { cache } from '../cache';
import { db } from '../db';
import { comments } from '../db/schema';
import { sql } from 'drizzle-orm';

import { getClusterData } from './clusters';
export { getClusterData };
export type { ClusterData, ClusterPoint, ClusterDataResponse } from './clusters';

/**
 * Initializes and tests database connection
 */
export async function initDatabase(): Promise<{ success: boolean, message: string, counts?: {comments: number} }> {
  try {
    // Test if we can connect by getting counts
    const result = await db.select({ count: sql<number>`count(*)` }).from(comments);
    
    return { 
      success: true, 
      message: "Database connection successful", 
      counts: {
        comments: result[0]?.count || 0
      }
    };
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error("Database connection error:", errorMessage);
    
    return { 
      success: false, 
      message: `Database connection failed: ${errorMessage}` 
    };
  }
}

/**
 * Clears all comment-related cache entries
 */
export async function clearCommentsCache(): Promise<void> {
  cache.deletePattern(/^comment-/);
  cache.deletePattern(/^comments-/);
  cache.deletePattern(/^stats-/);
}