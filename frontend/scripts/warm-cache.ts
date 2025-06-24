// src/scripts/warm-cache.ts
import { getPaginatedComments, getCommentStatistics } from '../src/lib/actions/comments';
import { cache } from '../src/lib/cache';
// import { serverLog } from '@/lib/serverLogger'; // Removed as unused

async function warmCache() {
  console.log('🔥 Starting cache warming...\n');
  
  const pageSizes = [10, 25, 50, 100];
  const stances = ['For', 'Against', 'Neutral/Unclear'];
  let warmedCount = 0;
  
  try {
    // Warm cache for different page sizes
    console.log('📄 Warming page size variations...');
    for (const pageSize of pageSizes) {
      const options = { page: 1, pageSize };
      await getPaginatedComments(options);
      await getCommentStatistics(options);
      warmedCount += 2;
      console.log(`  ✓ Page size ${pageSize}`);
    }
    
    // Warm cache for different stances
    console.log('\n🎯 Warming stance filter variations...');
    for (const stance of stances) {
      const options = { 
        page: 1, 
        pageSize: 10, 
        filters: { stance } 
      };
      await getPaginatedComments(options);
      await getCommentStatistics(options);
      warmedCount += 2;
      console.log(`  ✓ Stance: ${stance}`);
    }
    
    // Warm cache for common combinations
    console.log('\n🔄 Warming common combinations...');
    for (const pageSize of [10, 25]) {
      for (const stance of stances) {
        const options = { 
          page: 1, 
          pageSize, 
          filters: { stance } 
        };
        await getPaginatedComments(options);
        warmedCount++;
        console.log(`  ✓ Page size ${pageSize} + Stance: ${stance}`);
      }
    }
    
    console.log(`\n✅ Cache warming complete! Warmed ${warmedCount} variations.`);
    console.log(`📊 Cache size: ${cache.size()} entries\n`);
    
  } catch (error) {
    console.error('❌ Error warming cache:', error);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  warmCache().then(() => process.exit(0));
}