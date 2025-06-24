// lib/db/seed.ts
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import * as schema from './schema';
import { NewComment, NewLookupTableEntry, NewClusterDescription, stanceEnum } from './schema';
import fs from 'fs';
import path from 'path';
import { sql } from 'drizzle-orm';
import { dbConfig } from './config';

// Define interface for the data structure from data.json
interface CommentDataItem {
  id: string;
  title: string;
  category: string;
  agency_id: string;
  comment: string;
  // original_comment: string; // Will be ignored
  has_attachments: boolean;
  link?: string;
  posted_date: string;
  received_date: string;
  lookup_id: string;
  truncated_text: string;
  text_source: string;
  comment_count: number;
  stance?: string;
  key_quote?: string;
  rationale?: string;
  themes?: string;
  corrected?: boolean;
  cluster_id?: string;
  pca_x?: number;
  pca_y?: number;
  // New fields
  comment_on?: string;
  submitter_name?: string;
  organization?: string;
  city?: string;
  state?: string;
  country?: string;
  document_type?: string;
  attachment_count?: number;
  attachment_urls?: string;
  attachment_titles?: string;
}

// Define interface for lookup table data
interface LookupTableItem {
  lookup_id: string;
  truncated_text: string;
  text_source: string;
  comment_ids: string[];
  comment_count: number;
  stance?: string;
  key_quote?: string;
  rationale?: string;
  themes?: string;
  corrected?: boolean;
  cluster_id?: string;
  pca_x?: number;
  pca_y?: number;
}

const main = async () => {
  // Show which database we're seeding using dbConfig
  console.log(`\n🌱 Seeding ${dbConfig.isProd ? 'PRODUCTION' : dbConfig.isPreprod ? 'PREPROD' : 'LOCAL'} database`);
  
  if (dbConfig.isProd) {
    console.warn('\n⚠️  WARNING: You are about to seed the PRODUCTION database!');
    console.warn('This will modify production data. Press Ctrl+C to cancel.\n');
    
    // Give user 5 seconds to cancel
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
  
  const databaseUrlForSeed = dbConfig.url;

  console.log(`\n🌱 Attempting to seed database using URL: ${databaseUrlForSeed}`);

  const client = postgres(databaseUrlForSeed, { max: 1 });
  const db = drizzle(client, { schema });

  console.log('Connected to database.');

  // Determine data path based on environment
  //const devDataPath = path.resolve(__dirname, '../../__tests__/test-data-5-25.json');
  const devDataPath = path.resolve(__dirname, '../../../../data/data.json');
  const prodDataPath = path.resolve(__dirname, '../../../../data/data.json');
  const lookupTablePath = path.resolve(__dirname, '../../../../data/lookup_table.json');
  const clusterDescriptionsPath = path.resolve(__dirname, '../../../../data/cluster/cluster_descriptions.json');
  
  const dataPath = dbConfig.isDev ? devDataPath : prodDataPath;
  
  console.log(`Reading comments data from: ${dataPath}`);
  console.log(`Reading lookup table data from: ${lookupTablePath}`);
  console.log(`Reading cluster descriptions from: ${clusterDescriptionsPath}`);

  let rawData;
  let lookupData;
  let clusterDescriptionsData;
  
  try {
    rawData = fs.readFileSync(dataPath, 'utf-8');
    lookupData = fs.readFileSync(lookupTablePath, 'utf-8');
    clusterDescriptionsData = fs.readFileSync(clusterDescriptionsPath, 'utf-8');
  } catch (error) {
    console.error('Error reading data files:', error);
    process.exit(1);
  }
  
  const jsonData: CommentDataItem[] = JSON.parse(rawData);
  const lookupTableData: LookupTableItem[] = JSON.parse(lookupData);
  const clusterDescriptions: Record<string, [string, string]> = JSON.parse(clusterDescriptionsData);

  console.log(`Found ${jsonData.length} comments to process.`);
  console.log(`Found ${lookupTableData.length} lookup table entries to process.`);
  console.log(`Found ${Object.keys(clusterDescriptions).length} cluster descriptions to process.`);

  // Step 1: Insert cluster descriptions first
  console.log('\n🏷️  Inserting cluster descriptions...');
  const clusterDescriptionsToInsert: NewClusterDescription[] = Object.entries(clusterDescriptions).map(
    ([clusterId, [title, description]]) => ({
      clusterId,
      title,
      description,
    })
  );

  if (clusterDescriptionsToInsert.length > 0) {
    try {
      await db.insert(schema.clusterDescriptions).values(clusterDescriptionsToInsert).onConflictDoUpdate({
        target: schema.clusterDescriptions.clusterId,
        set: {
          title: sql`excluded.title`,
          description: sql`excluded.description`,
          updatedAt: sql`now()`
        }
      });
      console.log(`Upserted ${clusterDescriptionsToInsert.length} cluster descriptions.`);
    } catch (error) {
      console.error('Error upserting cluster descriptions:', error);
    }
  }

  // Step 2: Insert lookup table data
  console.log('\n📋 Inserting lookup table entries...');
  const lookupEntriesToInsert: NewLookupTableEntry[] = [];

  for (const item of lookupTableData) {
    // Validate the stance value
    const stance = item.stance 
      ? stanceEnum.enumValues.includes(item.stance as typeof stanceEnum.enumValues[number]) 
        ? item.stance as typeof stanceEnum.enumValues[number]
        : null
      : null;

    if (item.stance && !stanceEnum.enumValues.includes(item.stance as typeof stanceEnum.enumValues[number])) {
      console.warn(`Invalid stance value: "${item.stance}" for lookup ID: ${item.lookup_id}. Setting to null.`);
    }

    const newLookupEntry: NewLookupTableEntry = {
      lookupId: item.lookup_id,
      truncatedText: item.truncated_text,
      textSource: item.text_source,
      commentIds: item.comment_ids,
      commentCount: item.comment_count,
      stance: stance,
      keyQuote: item.key_quote,
      rationale: item.rationale,
      themes: item.themes,
      corrected: item.corrected || false,
      clusterId: item.cluster_id,
      pcaX: item.pca_x,
      pcaY: item.pca_y,
    };
    lookupEntriesToInsert.push(newLookupEntry);
  }

  // Insert lookup table entries in chunks
  if (lookupEntriesToInsert.length > 0) {
    console.log(`Inserting ${lookupEntriesToInsert.length} lookup table entries...`);
    const chunkSize = 100;
    for (let i = 0; i < lookupEntriesToInsert.length; i += chunkSize) {
      const chunk = lookupEntriesToInsert.slice(i, i + chunkSize);
      try {
        await db.insert(schema.lookupTable).values(chunk).onConflictDoUpdate({ 
          target: schema.lookupTable.lookupId, 
          set: { 
            truncatedText: sql`excluded.truncated_text`,
            textSource: sql`excluded.text_source`,
            commentIds: sql`excluded.comment_ids`,
            commentCount: sql`excluded.comment_count`,
            stance: sql`excluded.stance`,
            keyQuote: sql`excluded.key_quote`,
            rationale: sql`excluded.rationale`,
            themes: sql`excluded.themes`,
            corrected: sql`excluded.corrected`,
            clusterId: sql`excluded.cluster_id`,
            pcaX: sql`excluded.pca_x`,
            pcaY: sql`excluded.pca_y`
          } 
        });
        console.log(`Upserted chunk ${i / chunkSize + 1} of ${Math.ceil(lookupEntriesToInsert.length / chunkSize)} for lookup table`);
      } catch (error) {
        console.error(`Error upserting lookup table chunk ${i / chunkSize + 1}:`, error);
      }
    }
    console.log('Upserting lookup table completed.');
  }

  // Step 2: Create a map of lookup_id to comment_ids for quick access
  const lookupMap = new Map<string, string[]>();
  lookupTableData.forEach(item => {
    lookupMap.set(item.lookup_id, item.comment_ids);
  });

  // Step 4: Process comments and calculate comment_count
  console.log('\n📝 Processing comments...');
  const commentsToInsert: NewComment[] = [];

  for (const item of jsonData) {
    // Calculate comment_count: number of duplicates excluding self
    let calculatedCommentCount = 1; // Default to 1 if no lookup
    if (item.lookup_id && lookupMap.has(item.lookup_id)) {
      const commentIds = lookupMap.get(item.lookup_id)!;
      // Count is total comments in the group minus 1 (excluding self)
      calculatedCommentCount = commentIds.length;
    }

    // Validate the stance value
    const stance = item.stance 
      ? stanceEnum.enumValues.includes(item.stance as typeof stanceEnum.enumValues[number]) 
        ? item.stance as typeof stanceEnum.enumValues[number]
        : null
      : null;

    if (item.stance && !stanceEnum.enumValues.includes(item.stance as typeof stanceEnum.enumValues[number])) {
      console.warn(`Invalid stance value: "${item.stance}" for comment ID: ${item.id}. Setting to null.`);
    }

    const newComment: NewComment = {
      id: item.id,
      title: item.title,
      category: item.category,
      comment: item.comment,
      hasAttachments: item.has_attachments,
      link: item.link,
      postedDate: item.posted_date ? new Date(item.posted_date) : null,
      receivedDate: item.received_date ? new Date(item.received_date) : null,
      lookupId: item.lookup_id,
      textSource: item.text_source,
      commentCount: calculatedCommentCount,
      stance: stance,
      keyQuote: item.key_quote,
      rationale: item.rationale,
      themes: item.themes,
      corrected: item.corrected || false,
      clusterId: item.cluster_id?.toString(),
      pcaX: item.pca_x,
      pcaY: item.pca_y,
      // New fields
      organization: item.organization || null,
      documentType: item.document_type || null,
      attachmentCount: item.attachment_count ?? 0,
      attachmentUrls: item.attachment_urls || null,
      attachmentTitles: item.attachment_titles || null,
      truncatedText: item.truncated_text || null,
    };
    commentsToInsert.push(newComment);
  }

  // Step 5: Insert comments in chunks
  if (commentsToInsert.length > 0) {
    console.log(`Inserting ${commentsToInsert.length} comments into the database...`);
    const chunkSize = 100;
    for (let i = 0; i < commentsToInsert.length; i += chunkSize) {
      const chunk = commentsToInsert.slice(i, i + chunkSize);
      try {
        await db.insert(schema.comments).values(chunk).onConflictDoUpdate({ 
          target: schema.comments.id, 
          set: { 
            title: sql`excluded.title`, 
            category: sql`excluded.category`, 
            comment: sql`excluded.comment`, 
            hasAttachments: sql`excluded.has_attachments`, 
            link: sql`excluded.link`,
            postedDate: sql`excluded.posted_date`,
            receivedDate: sql`excluded.received_date`,
            lookupId: sql`excluded.lookup_id`,
            textSource: sql`excluded.text_source`,
            commentCount: sql`excluded.comment_count`,
            stance: sql`excluded.stance`,
            keyQuote: sql`excluded.key_quote`,
            rationale: sql`excluded.rationale`,
            themes: sql`excluded.themes`,
            corrected: sql`excluded.corrected`,
            clusterId: sql`excluded.cluster_id`,
            pcaX: sql`excluded.pca_x`,
            pcaY: sql`excluded.pca_y`,
            // New fields
            organization: sql`excluded.organization`,
            documentType: sql`excluded.document_type`,
            attachmentCount: sql`excluded.attachment_count`,
            attachmentUrls: sql`excluded.attachment_urls`,
            attachmentTitles: sql`excluded.attachment_titles`,
            truncatedText: sql`excluded.truncated_text`,
          } 
        });
        console.log(`Upserted chunk ${i / chunkSize + 1} of ${Math.ceil(commentsToInsert.length / chunkSize)} for comments`);
      } catch (error) {
        console.error(`Error upserting comments chunk ${i / chunkSize + 1}:`, error);
      }
    }
    console.log('Upserting comments completed.');
  } else {
    console.log('No comments found to seed.');
  }

  // Step 6: Verify insertion by counting records
  try {
    const lookupResult = await db.select({ count: sql`count(*)` }).from(schema.lookupTable);
    const lookupCount = lookupResult[0]?.count || 0;
    console.log(`\nVerification: Found ${lookupCount} records in the lookup_table.`);

    const commentResult = await db.select({ count: sql`count(*)` }).from(schema.comments);
    const commentCount = commentResult[0]?.count || 0;
    console.log(`Verification: Found ${commentCount} records in the comments table.`);

    const clusterDescResult = await db.select({ count: sql`count(*)` }).from(schema.clusterDescriptions);
    const clusterDescCount = clusterDescResult[0]?.count || 0;
    console.log(`Verification: Found ${clusterDescCount} records in the cluster_descriptions table.`);
  } catch (error) {
    console.error('Error verifying counts:', error);
  }

  await client.end();
  console.log('Database connection closed.');
};

main().catch((err) => {
  console.error('Error during seeding process:', err);
  process.exit(1);
});