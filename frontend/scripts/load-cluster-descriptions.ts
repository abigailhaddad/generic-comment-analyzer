#!/usr/bin/env tsx
// scripts/load-cluster-descriptions.ts
import { config } from 'dotenv';
import fs from 'fs';
import path from 'path';

// Load environment variables before importing DB modules
config(); // Load .env if it exists
config({ path: path.resolve(process.cwd(), '.env.local') }); // Load .env.local

// Now import DB modules after environment is loaded
import { db, connectDb } from '../src/lib/db';
import { clusterDescriptions, NewClusterDescription } from '../src/lib/db/schema';

async function loadClusterDescriptions() {
  try {
    // Connect to database
    const connection = await connectDb();
    if (!connection.success) {
      throw new Error("Failed to connect to database");
    }

    // Read the cluster descriptions JSON file
    const jsonPath = path.join(process.cwd(), '..', 'data', 'cluster', 'cluster_descriptions.json');
    
    if (!fs.existsSync(jsonPath)) {
      throw new Error(`Cluster descriptions file not found at: ${jsonPath}`);
    }

    const fileContent = fs.readFileSync(jsonPath, 'utf-8');
    const descriptions = JSON.parse(fileContent) as Record<string, [string, string]>;

    console.log(`Found ${Object.keys(descriptions).length} cluster descriptions to load`);

    // Transform the data into NewClusterDescription format
    const clusterDescriptionsData: NewClusterDescription[] = Object.entries(descriptions).map(
      ([clusterId, [title, description]]) => ({
        clusterId,
        title,
        description,
      })
    );

    // Clear existing data and insert new data
    console.log('Clearing existing cluster descriptions...');
    await db.delete(clusterDescriptions).execute();

    console.log('Inserting new cluster descriptions...');
    await db.insert(clusterDescriptions).values(clusterDescriptionsData).execute();

    console.log(`Successfully loaded ${clusterDescriptionsData.length} cluster descriptions into the database`);

    // Verify the data was inserted
    const insertedCount = await db.select().from(clusterDescriptions).execute();
    console.log(`Verification: ${insertedCount.length} records found in cluster_descriptions table`);

  } catch (error) {
    console.error('Error loading cluster descriptions:', error);
    process.exit(1);
  }
}

// Run the script if called directly
if (require.main === module) {
  loadClusterDescriptions()
    .then(() => {
      console.log('Cluster descriptions loaded successfully');
      process.exit(0);
    })
    .catch((error) => {
      console.error('Failed to load cluster descriptions:', error);
      process.exit(1);
    });
}

export { loadClusterDescriptions };