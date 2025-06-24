// scripts/switch-db.ts
import fs from 'fs';
import path from 'path';
import { config } from 'dotenv';

// Load current environment
config();
config({ path: path.resolve(process.cwd(), '.env.local') });

const envPath = path.resolve(process.cwd(), '.env');
const envLocalPath = path.resolve(process.cwd(), '.env.local');

// Get the target environment from command line
const targetEnv = process.argv[2]?.toLowerCase();

if (targetEnv !== 'local' && targetEnv !== 'preprod' && targetEnv !== 'prod') {
  console.error('❌ Please specify "local", "preprod", or "prod" as an argument');
  console.log('Usage: npm run db:use:local   OR   npm run db:use:preprod   OR   npm run db:use:prod');
  process.exit(1);
}

// Determine which file to update based on environment
const fileToUpdate = targetEnv === 'local' ? envLocalPath : envPath;

// Read the current file
let envContent = '';
try {
  envContent = fs.readFileSync(fileToUpdate, 'utf8');
} catch (error) {
  // If .env.local doesn't exist and we're switching to local, create it
  if (targetEnv === 'local' && !fs.existsSync(envLocalPath)) {
    envContent = `DATABASE_URL_LOCAL=postgresql://postgres:localdevpassword@localhost:5432/schedule_f_dev\nDB_ENV=local\n`;
  } else {
    console.error(`❌ Could not read ${fileToUpdate}:`, error);
    process.exit(1);
  }
}

// Update or add the DB_ENV line
if (envContent.includes('DB_ENV=')) {
  envContent = envContent.replace(/^DB_ENV=.*/m, `DB_ENV=${targetEnv}`);
} else {
  envContent += `\nDB_ENV=${targetEnv}\n`;
}

// Write back to the appropriate file
try {
  fs.writeFileSync(fileToUpdate, envContent);
  console.log(`✅ Switched to ${targetEnv.toUpperCase()} database`);
  console.log(`🗄️  DB_ENV is now set to: ${targetEnv}`);
  
  // Show appropriate warnings
  if (targetEnv === 'prod') {
    console.warn('\n⚠️  WARNING: You are now using the PRODUCTION database!');
    console.warn('Be careful with any operations that modify data.\n');
  } else if (targetEnv === 'preprod') {
    console.log('\n📋 You are now using the PRE-PRODUCTION database');
    console.log('This is your staging environment for testing before production.\n');
  } else if (targetEnv === 'local') {
    console.log('\n🏠 You are now using the LOCAL database');
    console.log('This is your Docker PostgreSQL instance for development.\n');
    
    // Check if Docker is running
    try {
      const { isContainerRunning } = require('./docker-utils');
      if (!isContainerRunning()) {
        console.log('💡 Tip: Start your local database with: npm run docker:start');
      }
    } catch {
      // docker-utils might not exist yet
    }
  }
} catch (error) {
  console.error(`❌ Could not update ${fileToUpdate}:`, error);
  process.exit(1);
}