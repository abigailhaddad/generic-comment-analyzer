// src/lib/actions/clusters.ts
'use server';

import { db, connectDb } from '@/lib/db';
import { comments, clusterDescriptions } from '@/lib/db/schema';
import { sql } from 'drizzle-orm';
import { unstable_cache } from 'next/cache';
import { cacheConfig } from '../cache-config';

export interface ClusterPoint {
  id: string;
  title: string;
  stance: string | null;
  clusterId: string;
  pcaX: number;
  pcaY: number;
  keyQuote: string | null;
  themes: string | null;
  clusterTitle?: string | null;
  clusterDescription?: string | null;
}

export interface ClusterData {
  clusters: Array<[string, ClusterPoint[]]>;
  bounds: {
    minX: number;
    maxX: number;
    minY: number;
    maxY: number;
  };
  totalPoints: number;
}

export interface ClusterDataResponse {
  success: boolean;
  data?: ClusterData;
  error?: string;
}

// Define an interface for the raw data returned by the database query
interface RawClusterPointRow {
  id: string;
  title: string | null;
  stance: string | null;
  clusterId: string | null;
  pcaX: number | null;
  pcaY: number | null;
  keyQuote: string | null;
  themes: string | null;
  clusterTitle: string | null;
  clusterDescription: string | null;
  // Allow for potential snake_case versions from direct SQL execution
  cluster_id?: string | null;
  pca_x?: number | null;
  pca_y?: number | null;
  key_quote?: string | null;
  cluster_title?: string | null;
  cluster_description?: string | null;
  // Allow for potential all-lowercase versions
  clusterid?: string | null;
  pcax?: number | null;
  pcay?: number | null;
  keyquote?: string | null;
}

export async function getClusterData(samplingFraction: number = 1.0): Promise<ClusterDataResponse> {
  const fetchClusterData = async () => {
    try {
      const connection = await connectDb();
      if (!connection.success) {
        throw new Error("Failed to connect to database");
      }

      // Fetch all cluster data with descriptions via LEFT JOIN
      const result = await db
        .select({
          id: comments.id,
          title: comments.title,
          stance: comments.stance,
          clusterId: comments.clusterId,
          pcaX: comments.pcaX,
          pcaY: comments.pcaY,
          keyQuote: comments.keyQuote,
          themes: comments.themes,
          clusterTitle: clusterDescriptions.title,
          clusterDescription: clusterDescriptions.description,
        })
        .from(comments)
        .leftJoin(clusterDescriptions, sql`${comments.clusterId} = ${clusterDescriptions.clusterId}`)
        .where(sql`${comments.clusterId} IS NOT NULL AND ${comments.pcaX} IS NOT NULL AND ${comments.pcaY} IS NOT NULL`)
        .execute();

      // Group by cluster ID using a Map
      const clustersMap = new Map<string, ClusterPoint[]>();
      let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;

      result.forEach((row: RawClusterPointRow) => {
        const point: ClusterPoint = {
          id: row.id,
          title: row.title || 'Untitled',
          stance: row.stance,
          clusterId: (row.cluster_id || row.clusterId || row.clusterid)!,
          pcaX: (row.pca_x || row.pcaX || row.pcax)!,
          pcaY: (row.pca_y || row.pcaY || row.pcay)!,
          keyQuote: (row.key_quote || row.keyQuote || row.keyquote) || null,
          themes: row.themes,
          clusterTitle: (row.cluster_title || row.clusterTitle) || null,
          clusterDescription: (row.cluster_description || row.clusterDescription) || null,
        };

        if (!clustersMap.has(point.clusterId)) {
          clustersMap.set(point.clusterId, []);
        }
        clustersMap.get(point.clusterId)!.push(point);

        // Update bounds
        minX = Math.min(minX, point.pcaX);
        maxX = Math.max(maxX, point.pcaX);
        minY = Math.min(minY, point.pcaY);
        maxY = Math.max(maxY, point.pcaY);
      });

      // Apply sampling if fraction is less than 1
      let sampledClustersArray: Array<[string, ClusterPoint[]]>;
      let sampledTotalPoints = 0;
      
      if (samplingFraction < 1.0) {
        sampledClustersArray = Array.from(clustersMap.entries()).map(([clusterId, points]) => {
          // Calculate how many points to sample from this cluster
          const sampleSize = Math.max(1, Math.floor(points.length * samplingFraction));
          
          // Randomly sample points
          const shuffled = [...points].sort(() => Math.random() - 0.5);
          const sampledPoints = shuffled.slice(0, sampleSize);
          
          sampledTotalPoints += sampledPoints.length;
          return [clusterId, sampledPoints];
        });
      } else {
        // No sampling, use all points
        sampledClustersArray = Array.from(clustersMap.entries());
        sampledTotalPoints = result.length;
      }

      return {
        success: true,
        data: {
          clusters: sampledClustersArray,
          bounds: { minX, maxX, minY, maxY },
          totalPoints: sampledTotalPoints,
        }
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error("Error fetching cluster data:", errorMessage);
      return {
        success: false,
        error: `Failed to fetch cluster data: ${errorMessage}`
      };
    }
  };

  // Use caching in production
  const shouldSkipCache = process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment;
  
  if (shouldSkipCache) {
    return fetchClusterData();
  }

  const getCachedClusterData = unstable_cache(
    fetchClusterData,
    [`cluster-data-${samplingFraction}`],
    {
      revalidate: 86400, // 24 hours
      tags: ['clusters']
    }
  );
  
  return getCachedClusterData();
}

// Get basic cluster statistics
export async function getClusterStats(): Promise<{
  success: boolean;
  stats?: {
    totalClusters: number;
    totalPoints: number;
    clusterSizes: { clusterId: string; size: number }[];
  };
  error?: string;
}> {
  const fetchStats = async () => {
    try {
      const connection = await connectDb();
      if (!connection.success) {
        throw new Error("Failed to connect to database");
      }

      const result = await db
        .select({
          clusterId: comments.clusterId,
          count: sql<number>`COUNT(*)`.mapWith(Number),
        })
        .from(comments)
        .where(sql`${comments.clusterId} IS NOT NULL`)
        .groupBy(comments.clusterId)
        .execute();

      const totalPoints = result.reduce((sum, row) => sum + row.count, 0);
      const clusterSizes = result.map(row => ({
        clusterId: row.clusterId!,
        size: row.count,
      }));

      return {
        success: true,
        stats: {
          totalClusters: result.length,
          totalPoints,
          clusterSizes,
        }
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        error: `Failed to fetch cluster stats: ${errorMessage}`
      };
    }
  };

  const shouldSkipCache = process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment;
  
  if (shouldSkipCache) {
    return fetchStats();
  }

  const getCachedStats = unstable_cache(
    fetchStats,
    ['cluster-stats'],
    {
      revalidate: 86400,
      tags: ['clusters']
    }
  );
  
  return getCachedStats();
}

// Get data for a single cluster
export async function getSingleClusterData(clusterId: string): Promise<ClusterDataResponse> {
  const fetchSingleClusterData = async () => {
    try {
      const connection = await connectDb();
      if (!connection.success) {
        throw new Error("Failed to connect to database");
      }

      // Fetch only the specific cluster data
      const result = await db
        .select({
          id: comments.id,
          title: comments.title,
          stance: comments.stance,
          clusterId: comments.clusterId,
          pcaX: comments.pcaX,
          pcaY: comments.pcaY,
          keyQuote: comments.keyQuote,
          themes: comments.themes,
          clusterTitle: clusterDescriptions.title,
          clusterDescription: clusterDescriptions.description,
        })
        .from(comments)
        .leftJoin(clusterDescriptions, sql`${comments.clusterId} = ${clusterDescriptions.clusterId}`)
        .where(sql`${comments.clusterId} = ${clusterId} AND ${comments.pcaX} IS NOT NULL AND ${comments.pcaY} IS NOT NULL`)
        .execute();

      if (result.length === 0) {
        return {
          success: false,
          error: `Cluster ${clusterId} not found`
        };
      }

      // Process the single cluster data
      const clusterPoints: ClusterPoint[] = [];
      let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;

      result.forEach((row: RawClusterPointRow) => {
        const point: ClusterPoint = {
          id: row.id,
          title: row.title || 'Untitled',
          stance: row.stance,
          clusterId: (row.cluster_id || row.clusterId || row.clusterid)!,
          pcaX: (row.pca_x || row.pcaX || row.pcax)!,
          pcaY: (row.pca_y || row.pcaY || row.pcay)!,
          keyQuote: (row.key_quote || row.keyQuote || row.keyquote) || null,
          themes: row.themes,
          clusterTitle: (row.cluster_title || row.clusterTitle) || null,
          clusterDescription: (row.cluster_description || row.clusterDescription) || null,
        };

        clusterPoints.push(point);

        // Update bounds
        minX = Math.min(minX, point.pcaX);
        maxX = Math.max(maxX, point.pcaX);
        minY = Math.min(minY, point.pcaY);
        maxY = Math.max(maxY, point.pcaY);
      });

      const clusters: [string, ClusterPoint[]][] = [[clusterId, clusterPoints]];
      
      return {
        success: true,
        data: {
          clusters,
          bounds: { minX, maxX, minY, maxY },
          totalPoints: result.length,
        }
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error("Error fetching single cluster data:", errorMessage);
      return {
        success: false,
        error: `Failed to fetch cluster data: ${errorMessage}`
      };
    }
  };

  // Use caching in production
  const shouldSkipCache = process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment;
  
  if (shouldSkipCache) {
    return fetchSingleClusterData();
  }

  const getCachedSingleClusterData = unstable_cache(
    fetchSingleClusterData,
    [`cluster-data-${clusterId}`],
    {
      revalidate: 86400, // 24 hours
      tags: ['clusters', `cluster-${clusterId}`]
    }
  );
  
  return getCachedSingleClusterData();
}