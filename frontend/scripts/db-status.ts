// scripts/db-status.ts
import { config } from 'dotenv';
import path from 'path';
import { sql } from 'drizzle-orm';

// Load environment variables
config({ path: path.resolve(process.cwd(), '.env') });
config({ path: path.resolve(process.cwd(), '.env.local') });

// Import after env vars are loaded
import { dbConfig } from '../src/lib/db/config';
import { connectDb, db } from '../src/lib/db';
import { comments } from '../src/lib/db/schema';

async function checkDatabaseStatus() {
  console.log('🔍 Database Status Check\n');
  
  // Determine environment
  let environment = dbConfig.env?.toUpperCase() || 'UNKNOWN';
  let emoji = '❓';
  
  switch (dbConfig.env) {
    case 'local':
      emoji = '🏠';
      break;
    case 'preprod':
      emoji = '🧪';
      break;
    case 'prod':
      emoji = '🚀';
      break;
  }
  
  console.log(`${emoji} Environment: ${environment}`);
  console.log(`Database URL: ${dbConfig.url.substring(0, 50)}...`);
  console.log('-------------------\n');

  // Check Docker status if local
  if (dbConfig.env === 'local') {
    try {
      const { isContainerRunning, isDockerRunning } = await import('./docker-utils');
      
      if (!isDockerRunning()) {
        console.log('🐳 Docker Status: NOT RUNNING');
        console.log('💡 Start Docker Desktop first, then run: npm run docker:start\n');
        process.exit(1);
      }
      
      if (!isContainerRunning()) {
        console.log('🐳 Docker Status: Running');
        console.log('📦 PostgreSQL Container: NOT RUNNING');
        console.log('💡 Start the container with: npm run docker:start\n');
        process.exit(1);
      }
      
      console.log('🐳 Docker Status: Running');
      console.log('📦 PostgreSQL Container: Running\n');
    } catch {
      // docker-utils might not exist
      console.log('🐳 Docker Status: Unable to check\n');
    }
  }

  try {
    // Try to connect
    console.log('Attempting connection...');
    const connection = await connectDb();
    
    if (!connection.success) {
      console.error('❌ Failed to connect to database');
      if (dbConfig.env === 'local') {
        console.log('\n💡 Make sure Docker is running: npm run docker:start');
      }
      return;
    }
    
    console.log('✅ Successfully connected to database\n');
    
    // Get some basic stats
    console.log('📊 Database Statistics:');
    
    // Get comment count
    const commentCount = await db.select({ count: sql<number>`count(*)` }).from(comments);
    console.log(`   Total Comments: ${commentCount[0]?.count || 0}`);
    
    // Get stance distribution
    const stanceStats = await db
      .select({
        stance: comments.stance,
        count: sql<number>`count(*)`
      })
      .from(comments)
      .groupBy(comments.stance);
    
    console.log('\n   Stance Distribution:');
    stanceStats.forEach(stat => {
      console.log(`   - ${stat.stance || 'Unknown'}: ${stat.count}`);
    });
    
    // Show last created comment date
    const lastComment = await db
      .select({ createdAt: comments.createdAt })
      .from(comments)
      .orderBy(sql`${comments.createdAt} DESC`)
      .limit(1);
    
    if (lastComment[0]) {
      console.log(`\n   Last Comment Added: ${new Date(lastComment[0].createdAt).toLocaleString()}`);
    }
    
    console.log('\n✅ Database check complete!');
    
  } catch (error) {
    console.error('❌ Error during database check:', error);
    if (dbConfig.env === 'local') {
      console.log('\n💡 Troubleshooting tips:');
      console.log('   1. Make sure Docker is running: docker ps');
      console.log('   2. Start the container: npm run docker:start');
      console.log('   3. Check logs: docker logs schedule-f-postgres');
    }
  }
   
  process.exit(0);
}

// Run the check
checkDatabaseStatus();