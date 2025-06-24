// scripts/debug-clusters.ts
import { config } from 'dotenv';
import path from 'path';
import { sql } from 'drizzle-orm';

// Load environment variables
//config({ path: path.resolve(process.cwd(), '.env') });
config({ path: path.resolve(process.cwd(), '.env.local') });

// Import after env vars are loaded
import { dbConfig } from '../src/lib/db/config';
import { connectDb, db } from '../src/lib/db';
import { comments } from '../src/lib/db/schema';

async function debugClusterData() {
  console.log('🔍 Debugging Cluster Data\n');
  console.log(`Environment: ${dbConfig.env?.toUpperCase()}`);
  console.log('-------------------\n');

  try {
    // Connect to database
    const connection = await connectDb();
    if (!connection.success) {
      console.error('❌ Failed to connect to database');
      return;
    }

    // 1. Check total comments
    const totalComments = await db.select({ count: sql<number>`count(*)` }).from(comments);
    console.log(`Total comments in database: ${totalComments[0]?.count || 0}`);

    // 2. Check comments with cluster IDs
    const withClusterIds = await db
      .select({ count: sql<number>`count(*)` })
      .from(comments)
      .where(sql`${comments.clusterId} IS NOT NULL`);
    console.log(`Comments with cluster IDs: ${withClusterIds[0]?.count || 0}`);

    // 3. Check comments with all cluster data
    const withFullClusterData = await db
      .select({ count: sql<number>`count(*)` })
      .from(comments)
      .where(sql`${comments.clusterId} IS NOT NULL AND ${comments.pcaX} IS NOT NULL AND ${comments.pcaY} IS NOT NULL`);
    console.log(`Comments with full cluster data (ID + PCA coords): ${withFullClusterData[0]?.count || 0}`);

    // 4. Sample some cluster data
    const sampleClusterData = await db
      .select({
        id: comments.id,
        clusterId: comments.clusterId,
        pcaX: comments.pcaX,
        pcaY: comments.pcaY,
      })
      .from(comments)
      .where(sql`${comments.clusterId} IS NOT NULL`)
      .limit(5);
    
    console.log('\nSample cluster data:');
    sampleClusterData.forEach(row => {
      console.log(`  ID: ${row.id}, Cluster: ${row.clusterId}, PCA: (${row.pcaX}, ${row.pcaY})`);
    });

    // 5. Get unique cluster IDs
    const uniqueClusters = await db
      .select({
        clusterId: comments.clusterId,
        count: sql<number>`COUNT(*)`.mapWith(Number),
      })
      .from(comments)
      .where(sql`${comments.clusterId} IS NOT NULL`)
      .groupBy(comments.clusterId);
    
    console.log(`\nUnique clusters: ${uniqueClusters.length}`);
    uniqueClusters.slice(0, 5).forEach(cluster => {
      console.log(`  Cluster ${cluster.clusterId}: ${cluster.count} comments`);
    });

    // 6. Check for any null PCA values in clustered comments
    const nullPcaInClusters = await db
      .select({ count: sql<number>`count(*)` })
      .from(comments)
      .where(sql`${comments.clusterId} IS NOT NULL AND (${comments.pcaX} IS NULL OR ${comments.pcaY} IS NULL)`);
    console.log(`\nComments with cluster ID but missing PCA coords: ${nullPcaInClusters[0]?.count || 0}`);

  } catch (error) {
    console.error('❌ Error during cluster data check:', error);
  }

  process.exit(0);
}

// Run the check
debugClusterData();