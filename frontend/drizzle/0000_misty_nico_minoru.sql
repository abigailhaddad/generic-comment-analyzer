CREATE TYPE "public"."stance" AS ENUM('For', 'Against', 'Neutral/Unclear');--> statement-breakpoint
CREATE TABLE "analyses" (
	"id" serial PRIMARY KEY NOT NULL,
	"comment_id" varchar,
	"stance" "stance",
	"key_quote" text,
	"rationale" text,
	"themes" text,
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "comments" (
	"id" varchar PRIMARY KEY NOT NULL,
	"title" text,
	"category" varchar,
	"agency_id" varchar,
	"comment" text,
	"original_comment" text,
	"has_attachments" boolean DEFAULT false,
	"link" text,
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
ALTER TABLE "analyses" ADD CONSTRAINT "analyses_comment_id_comments_id_fk" FOREIGN KEY ("comment_id") REFERENCES "public"."comments"("id") ON DELETE no action ON UPDATE no action;