// src/lib/db/config.ts
import { config } from 'dotenv';

// Load environment variables - simplified without path module
const envPath = process.env.ENV_PATH || '.env';
config({ path: envPath });

// Database environment type - now supports 3 environments
type DbEnvironment = 'local' | 'preprod' | 'prod';

// Get the database environment
const getDbEnvironment = (): DbEnvironment => {
  const dbEnv = process.env.DB_ENV?.toLowerCase();

  // Validate the environment
  if (dbEnv !== 'local' && dbEnv !== 'preprod' && dbEnv !== 'prod') {
    console.warn(`Invalid DB_ENV: "${dbEnv}". Defaulting to "local".`);
    return 'local';
  }

  return dbEnv;
};

// Get the appropriate database URL
const getDatabaseUrl = (): string => {
  const dbEnv = getDbEnvironment();
  
  // Map environment to URL
  const urlMap = {
    local: process.env.DATABASE_URL_LOCAL,
    preprod: process.env.DATABASE_URL_PREPROD,
    prod: process.env.DATABASE_URL_PROD
  };
  
  const dbUrl = urlMap[dbEnv];
  if (!dbUrl) {
    throw new Error(`DATABASE_URL_${dbEnv.toUpperCase()} is not defined in environment variables`);
  }

  // Log which database we're using (but not the URL for security)
  if (typeof window === 'undefined' && process.env.NODE_ENV === 'development') {
    console.log(`🗄️  Using ${dbEnv.toUpperCase()} database`);
  }

  return dbUrl;
};

// Export the configuration
const getDbConfig = () => {
  const dbUrl = getDatabaseUrl();
  const dbEnv = getDbEnvironment();

  return { 
    url: dbUrl, 
    isProd: dbEnv === 'prod', 
    isPreprod: dbEnv === 'preprod',
    isLocal: dbEnv === 'local',
    isDev: dbEnv === 'local', // Backward compatibility
    env: dbEnv
  };
};

export const dbConfig = getDbConfig();