-- Simple single-table PostgreSQL schema for comment analysis

CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    
    -- Basic comment info
    comment_id VARCHAR(255) UNIQUE NOT NULL,
    submitter_name VARCHAR(255),
    organization VARCHAR(255),
    submission_date TIMESTAMP,
    
    -- Text content
    comment_text TEXT,
    attachment_text TEXT,
    combined_text TEXT,
    
    -- Analysis results
    stance VARCHAR(100),
    themes TEXT[], -- PostgreSQL array of theme strings
    key_quote TEXT,
    rationale TEXT,
    
    -- Metadata
    has_attachments BOOLEAN DEFAULT FALSE,
    model_used VARCHAR(100),
    regulation_name VARCHAR(255),
    docket_id VARCHAR(255),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Simple indexes
CREATE INDEX idx_comments_comment_id ON comments(comment_id);
CREATE INDEX idx_comments_stance ON comments(stance);
CREATE INDEX idx_comments_regulation ON comments(regulation_name);
CREATE INDEX idx_comments_created_at ON comments(created_at);