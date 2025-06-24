// scripts/ensure-local-db.ts
//!/usr/bin/env tsx
import { startPostgresContainer } from './docker-utils';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

// Check if .env.local exists, if not create it
const envLocalPath = path.join(process.cwd(), '.env.local');
const envPath = path.join(process.cwd(), '.env');

if (!fs.existsSync(envLocalPath)) {
  console.log('📝 Creating .env.local file...');
  
  // Read existing .env if it exists
  let envContent = '';
  if (fs.existsSync(envPath)) {
    envContent = fs.readFileSync(envPath, 'utf-8');
  }
  
  // Add local database URL
  const localDbUrl = `DATABASE_URL_LOCAL=postgresql://postgres:localdevpassword@localhost:5432/schedule_f_dev`;
  
  // Check if we need to rename existing DEV to PREPROD
  if (envContent.includes('DATABASE_URL_DEV=')) {
    envContent = envContent.replace('DATABASE_URL_DEV=', 'DATABASE_URL_PREPROD=');
    console.log('📝 Renamed DATABASE_URL_DEV to DATABASE_URL_PREPROD');
  }
  
  // Write new .env.local
  fs.writeFileSync(envLocalPath, `${envContent}\n${localDbUrl}\n`);
  console.log('✅ Created .env.local with local database configuration');
}

// Start PostgreSQL container
try {
  startPostgresContainer();
  
  // Set environment to local
  process.env.DB_ENV = 'local';
  
  console.log('✅ Local development database is ready!');
  console.log('📌 Connection string: postgresql://postgres:localdevpassword@localhost:5432/schedule_f_dev');
} catch (error) {
  console.error('❌ Failed to start local database:', error);
  process.exit(1);
}