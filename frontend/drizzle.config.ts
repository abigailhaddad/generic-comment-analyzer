// drizzle.config.ts
import { defineConfig } from 'drizzle-kit';
import { config } from 'dotenv';
import path from 'path';

// Load environment variables from both .env and .env.local
config({ path: path.resolve(process.cwd(), '.env') });
config({ path: path.resolve(process.cwd(), '.env.local') });

// Get the database environment
const dbEnv = process.env.DB_ENV?.toLowerCase() || 'local';

// Get the appropriate database URL
const getDatabaseUrl = (): string => {
  let dbUrl: string | undefined;
  
  switch (dbEnv) {
    case 'prod':
      dbUrl = process.env.DATABASE_URL_PROD;
      break;
    case 'preprod':
      dbUrl = process.env.DATABASE_URL_PREPROD;
      break;
    case 'local':
    default:
      dbUrl = process.env.DATABASE_URL_LOCAL;
      break;
  }
  
  if (!dbUrl) {
    throw new Error(`DATABASE_URL_${dbEnv.toUpperCase()} is not defined in environment variables`);
  }
  
  console.log(`🗄️  Drizzle using ${dbEnv.toUpperCase()} database`);
  
  return dbUrl;
};

export default defineConfig({
  schema: './src/lib/db/schema.ts',
  out: './drizzle',
  dialect: 'postgresql',
  dbCredentials: {
    url: getDatabaseUrl()
  }
});