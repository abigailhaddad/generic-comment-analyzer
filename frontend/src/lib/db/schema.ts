// lib/db/schema.ts
import { pgTable, text, timestamp, varchar, boolean, pgEnum, integer, doublePrecision, json } from 'drizzle-orm/pg-core';

export const stanceEnum = pgEnum('stance', ['For', 'Against', 'Neutral/Unclear']);

// Cluster descriptions table for storing human-readable cluster information
export const clusterDescriptions = pgTable('cluster_descriptions', {
  clusterId: text('cluster_id').primaryKey(),
  title: text('title').notNull(),
  description: text('description').notNull(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
  updatedAt: timestamp('updated_at').defaultNow().notNull()
});

// Lookup table for grouping duplicate comments
export const lookupTable = pgTable('lookup_table', {
  lookupId: varchar('lookup_id').primaryKey(),
  truncatedText: text('truncated_text'),
  textSource: varchar('text_source'),
  commentIds: json('comment_ids').$type<string[]>().notNull(),
  commentCount: integer('comment_count').notNull(),
  stance: stanceEnum('stance'),
  keyQuote: text('key_quote'),
  rationale: text('rationale'),
  themes: text('themes'),
  corrected: boolean('corrected').default(false),
  clusterId: text('cluster_id'),
  pcaX: doublePrecision('pca_x'),
  pcaY: doublePrecision('pca_y'),
  createdAt: timestamp('created_at').defaultNow().notNull()
});

export const comments = pgTable('comments', {
  id: varchar('id').primaryKey(),
  title: text('title'),
  category: varchar('category'),
  comment: text('comment'),
  // originalComment removed - not needed
  hasAttachments: boolean('has_attachments').default(false),
  link: text('link'),
  postedDate: timestamp('posted_date'),
  receivedDate: timestamp('received_date'),
  
  // Link to lookup table
  lookupId: varchar('lookup_id').references(() => lookupTable.lookupId),
  
  // These fields are now denormalized from lookup table for performance
  textSource: varchar('text_source'),
  commentCount: integer('comment_count').default(1),
  stance: stanceEnum('stance'),
  keyQuote: text('key_quote'),
  rationale: text('rationale'),
  themes: text('themes'),
  corrected: boolean('corrected').default(false),
  clusterId: text('cluster_id'),
  pcaX: doublePrecision('pca_x'),
  pcaY: doublePrecision('pca_y'),
  
  createdAt: timestamp('created_at').defaultNow().notNull(),
  organization: varchar('organization'),
  documentType: varchar('document_type'),
  attachmentUrls: text('attachment_urls'), //This will be somma separated with a space, example: "https://downloads.regulations.gov/OPM-2025-0004-0645/attachment_1.docx; https://downloads.regulations.gov/OPM-2025-0004-0645/attachment_1.pdf",
  attachmentTitles: text('attachment_titles'), //An example:     "attachment_titles": "attachment_1.docx; attachment_1.pdf",

  attachmentCount: integer('attachment_count'),
  truncatedText: text('truncated_text'),
});

// Type inference
export type Comment = typeof comments.$inferSelect;
export type NewComment = typeof comments.$inferInsert;
export type LookupTableEntry = typeof lookupTable.$inferSelect;
export type NewLookupTableEntry = typeof lookupTable.$inferInsert;
export type ClusterDescription = typeof clusterDescriptions.$inferSelect;
export type NewClusterDescription = typeof clusterDescriptions.$inferInsert;