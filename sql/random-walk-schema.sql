-- =============================================================================
-- Random Walk Crawler — PostgreSQL Schema
-- =============================================================================
-- Roles referenced in GRANT statements (create these before applying):
--
--   crawler_app       The DAPR postgres-binding identity used by all crawler
--                     pods at runtime (DML + sequence access).
--
--   crawler_readonly  Read-only role for analytics, BI, and monitoring queries.
--                     Grant to individual analysts via: GRANT crawler_readonly TO <user>;
--
--   crawler_migration Schema-owner / migration role. Must own the tables or hold
--                     SUPERUSER to run DDL. Only used during deploys — never by
--                     application pods.
--
-- NOTE: edge_records uses a SERIAL (sequence) primary key. INSERT requires
-- USAGE on the generated sequence object (edge_records_edge_id_seq), not just
-- INSERT on the table. This is granted explicitly in the grants section below.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- edge_records
-- Records each directed edge discovered during a crawl walk.
-- One row per (source → destination) observation; duplicates are intentional
-- in random-walk mode — multiple sources in the same layer may link to the
-- same destination and each observation is a distinct data point.
-- ---------------------------------------------------------------------------
CREATE TABLE edge_records (
    edge_id            SERIAL       PRIMARY KEY,
    destination_channel VARCHAR(64) NOT NULL,
    source_channel     VARCHAR(64)  NOT NULL,
    walkback           BOOLEAN      NOT NULL,
    skipped            BOOLEAN      NOT NULL,
    discovery_time     TIMESTAMP    NOT NULL,
    crawl_id           VARCHAR(64)  NOT NULL,
    sequence_id        VARCHAR(36)  NOT NULL DEFAULT ''
    -- sequence_id: UUID shared across all edges in one uninterrupted forward
    -- walk chain (A→B→C). A walkback generates a fresh UUID for the new chain.
    -- Empty string means sequence tracking not used for this row.
);

-- Common query: fetch all edges for a crawl session
CREATE INDEX idx_edge_records_crawl_id
    ON edge_records (crawl_id);

-- Common query: find all outbound edges from a source channel
CREATE INDEX idx_edge_records_source_channel
    ON edge_records (source_channel);

-- Chain analysis: look up all edges belonging to one sequence
CREATE INDEX idx_edge_records_sequence_id
    ON edge_records (sequence_id)
    WHERE sequence_id <> '';

-- Time-range analytics
CREATE INDEX idx_edge_records_discovery_time
    ON edge_records (discovery_time);

-- Composite: per-crawl source lookup (covers most runtime query patterns)
CREATE INDEX idx_edge_records_crawl_source
    ON edge_records (crawl_id, source_channel);


-- ---------------------------------------------------------------------------
-- page_buffer
-- Transient queue of pages to process in the next BFS/random-walk layer.
-- Scoped per pod via crawl_id — each pod only reads/writes its own rows.
-- Rows are deleted after a layer completes; this table should stay small.
-- ---------------------------------------------------------------------------
CREATE TABLE page_buffer (
    page_id    VARCHAR(36)  PRIMARY KEY,     -- UUID
    parent_id  VARCHAR(36)  NOT NULL,        -- UUID of parent page
    depth      INTEGER      NOT NULL,
    url        VARCHAR(64)  NOT NULL,        -- channel username
    crawl_id   VARCHAR(64)  NOT NULL,
    sequence_id VARCHAR(36) NOT NULL DEFAULT ''
);

-- All runtime queries filter on crawl_id (pod isolation)
CREATE INDEX idx_page_buffer_crawl_id
    ON page_buffer (crawl_id);


-- ---------------------------------------------------------------------------
-- seed_channels
-- Canonical list of seed channels for random-walk crawls (~500k rows).
-- Dual-purpose: initial seed pool + chat ID cache (avoids redundant TDLib
-- SearchPublicChat RPCs) + last-crawl watermark (delta-fetch on revisit).
-- ---------------------------------------------------------------------------
CREATE TABLE seed_channels (
    channel_username   VARCHAR(64)  PRIMARY KEY,
    chat_id            BIGINT,               -- cached TDLib chat ID; NULL = not yet resolved
    last_crawled_at    TIMESTAMP,            -- NULL = never crawled; set by MarkChannelCrawled()
    invalidated_at     TIMESTAMP,            -- NULL = valid; set by MarkSeedChannelInvalid(); 30-day TTL
    member_count       INTEGER,
    inserted_at        TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Seed selection: quickly find channels never crawled (NULL first)
CREATE INDEX idx_seed_channels_last_crawled
    ON seed_channels (last_crawled_at NULLS FIRST);

-- Partial index for the common "pick uncrawled seeds" pattern
CREATE INDEX idx_seed_channels_uncrawled
    ON seed_channels (inserted_at)
    WHERE last_crawled_at IS NULL;


-- ---------------------------------------------------------------------------
-- invalid_channels
-- Shared cache of channel usernames that failed SearchPublicChat validation.
-- TTL of 30 days is enforced in application logic (IsInvalidChannel checks
-- invalidated_at). Shared across all pods to reduce redundant API calls.
-- ---------------------------------------------------------------------------
CREATE TABLE invalid_channels (
    channel_username   VARCHAR(64)  PRIMARY KEY,
    reason             VARCHAR(64)  NOT NULL DEFAULT '',   -- e.g. "not_found", "not_supergroup"
    invalidated_at     TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- TTL query: load only non-expired rows on startup
CREATE INDEX idx_invalid_channels_invalidated_at
    ON invalid_channels (invalidated_at);


-- =============================================================================
-- GRANTS
-- =============================================================================

-- ---------------------------------------------------------------------------
-- crawler_app (runtime DAPR binding user)
-- Needs full DML on all tables plus USAGE on the SERIAL sequence so that
-- INSERT into edge_records can call nextval().
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE edge_records      TO crawler_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE page_buffer      TO crawler_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE seed_channels     TO crawler_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE invalid_channels  TO crawler_app;

-- SERIAL sequence: USAGE is required to call nextval(); SELECT for currval()
GRANT USAGE, SELECT ON SEQUENCE edge_records_edge_id_seq TO crawler_app;

-- ---------------------------------------------------------------------------
-- crawler_readonly (analytics / monitoring)
-- SELECT only — no sequence access needed.
-- ---------------------------------------------------------------------------
GRANT SELECT ON TABLE edge_records      TO crawler_readonly;
GRANT SELECT ON TABLE page_buffer      TO crawler_readonly;
GRANT SELECT ON TABLE seed_channels     TO crawler_readonly;
GRANT SELECT ON TABLE invalid_channels  TO crawler_readonly;

-- ---------------------------------------------------------------------------
-- crawler_migration (schema owner — runs DDL during deploys)
-- Owns all objects; grants above flow from ownership.
-- In practice: run migrations as crawler_migration or a superuser role.
-- No explicit GRANT needed here if crawler_migration owns the tables,
-- but listed for documentation purposes.
-- ---------------------------------------------------------------------------
-- GRANT ALL ON ALL TABLES IN SCHEMA public TO crawler_migration;
-- GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO crawler_migration;


-- =============================================================================
-- MIGRATION STATEMENTS (existing deployments)
-- Run these against a live DB that has the original schema.
-- =============================================================================

-- Add sequence_id columns (if upgrading from pre-sequence schema):
-- ALTER TABLE edge_records  ADD COLUMN IF NOT EXISTS sequence_id VARCHAR(36) NOT NULL DEFAULT '';
-- ALTER TABLE page_buffer  ADD COLUMN IF NOT EXISTS sequence_id VARCHAR(36) NOT NULL DEFAULT '';

-- Widen channel username columns (VARCHAR(32) → VARCHAR(64)):
-- ALTER TABLE edge_records  ALTER COLUMN destination_channel TYPE VARCHAR(64);
-- ALTER TABLE edge_records  ALTER COLUMN source_channel      TYPE VARCHAR(64);
-- ALTER TABLE edge_records  ALTER COLUMN crawl_id            TYPE VARCHAR(64);
-- ALTER TABLE page_buffer  ALTER COLUMN url                 TYPE VARCHAR(64);
-- ALTER TABLE page_buffer  ALTER COLUMN crawl_id            TYPE VARCHAR(64);

-- Add invalid_channels table (if it doesn't exist):
-- CREATE TABLE IF NOT EXISTS invalid_channels (
--     channel_username VARCHAR(64) PRIMARY KEY,
--     reason           VARCHAR(64) NOT NULL DEFAULT '',
--     invalidated_at   TIMESTAMP   NOT NULL DEFAULT NOW()
-- );

-- Add indexes (IF NOT EXISTS guards make these safe to replay):
-- CREATE INDEX IF NOT EXISTS idx_edge_records_crawl_id       ON edge_records (crawl_id);
-- CREATE INDEX IF NOT EXISTS idx_edge_records_source_channel ON edge_records (source_channel);
-- CREATE INDEX IF NOT EXISTS idx_edge_records_sequence_id    ON edge_records (sequence_id) WHERE sequence_id <> '';
-- CREATE INDEX IF NOT EXISTS idx_edge_records_discovery_time ON edge_records (discovery_time);
-- CREATE INDEX IF NOT EXISTS idx_edge_records_crawl_source   ON edge_records (crawl_id, source_channel);
-- CREATE INDEX IF NOT EXISTS idx_page_buffer_crawl_id       ON page_buffer (crawl_id);
-- CREATE INDEX IF NOT EXISTS idx_seed_channels_last_crawled  ON seed_channels (last_crawled_at NULLS FIRST);
-- CREATE INDEX IF NOT EXISTS idx_seed_channels_uncrawled     ON seed_channels (inserted_at) WHERE last_crawled_at IS NULL;
-- CREATE INDEX IF NOT EXISTS idx_invalid_channels_invalidated_at ON invalid_channels (invalidated_at);

-- Grant new sequence permission to existing app user:
-- GRANT USAGE, SELECT ON SEQUENCE edge_records_edge_id_seq TO crawler_app;
