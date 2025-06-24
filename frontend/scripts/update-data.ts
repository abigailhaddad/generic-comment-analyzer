// scripts/update-data.ts
import { config } from 'dotenv';
import path from 'path';

// Load environment variables from both files
config({ path: path.resolve(process.cwd(), '.env') });
config({ path: path.resolve(process.cwd(), '.env.local') });

async function updateData() {
  const dbEnv = process.env.DB_ENV || 'local';
  console.log(`📊 Starting data update process for ${dbEnv.toUpperCase()} environment...`);
  
  // Safety check
  if (dbEnv === 'prod') {
    console.warn('\n⚠️  WARNING: You are about to update PRODUCTION data!');
    console.warn('Press Ctrl+C within 5 seconds to cancel...\n');
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
  
  try {
    // 1. Run your data update logic here
    console.log('Running database seed...');
    
    // Dynamic import of seed script with proper environment
    process.env.DB_ENV = dbEnv;
    await import('../src/lib/db/seed');
    
    console.log('Data update completed successfully');
    
    // 2. Trigger revalidation if you have a deployment URL
    const deploymentUrl = process.env.DEPLOYMENT_URL || 'http://localhost:3000';
    const revalidationToken = process.env.REVALIDATION_TOKEN;
    
    if (deploymentUrl && dbEnv === 'prod') {
      console.log('Triggering page revalidation...');
      
      const response = await fetch(`${deploymentUrl}/api/revalidate`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(revalidationToken && { 'Authorization': `Bearer ${revalidationToken}` })
        }
      });
      
      if (response.ok) {
        const data = await response.json();
        console.log('Revalidation triggered successfully:', data);
      } else {
        console.error('Failed to trigger revalidation:', response.status);
      }
    }
    
    // 3. Update the LAST_DATA_UPDATE
    const timestamp = new Date().toISOString();
    console.log(`
✅ Data update complete!

Remember to update LAST_DATA_UPDATE in your .env file:
LAST_DATA_UPDATE=${timestamp}

Environment: ${dbEnv.toUpperCase()}
Timestamp: ${timestamp}
    `);
    
  } catch (error) {
    console.error('Error during data update:', error);
    process.exit(1);
  }
}

// Run the update
updateData();