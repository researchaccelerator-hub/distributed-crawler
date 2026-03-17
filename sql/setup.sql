-- =============================================================================
-- Random Walk Crawler — PostgreSQL Setup Script
-- =============================================================================
-- Each table is fully configured (CREATE TABLE → indexes → grants) before the
-- next table is created. All statements are idempotent (IF NOT EXISTS / DO
-- NOTHING) so this script is safe to re-run during redeployments.
--
-- Target: Azure Database for PostgreSQL Flexible Server
-- Auth:   Azure Entra ID (managed identity) via pgaadauth extension
--
-- BEFORE RUNNING THIS SCRIPT:
--   1. The executing identity must be the Entra ID admin for the Flexible
--      Server (set in Azure portal → PostgreSQL → Authentication).
--   2. Substitute the two placeholder identity names below:
--        <crawler-app-managed-identity>   — user-assigned managed identity
--                                           attached to all crawler pods
--        <crawler-readonly-identity>      — optional; omit the block if not
--                                           used (e.g., a human analyst account
--                                           or a separate MI for monitoring)
--   3. Uncomment line for seeding seed_channel. Substitute place holder for path to seed files. 
--        <absolute_path_to_seed_channels> - csv file with just usernames, header included
--
-- Run with:
--   psql "$DATABASE_URL" -f sql/setup.sql
-- =============================================================================


-- =============================================================================
-- ROLE SETUP
-- Must complete before any table is created.
--
-- Two-layer model:
--   Permission roles  (NOLOGIN) — own the GRANTs; attached to identities.
--   Azure AD principals          — the actual login identities; granted a role.
--
-- crawler_migration is NOT created here — it IS the identity running this
-- script (the deployment pipeline's managed identity / Entra ID admin).
-- =============================================================================

-- crawler_app: runtime DML role for all crawler pods.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'crawler_app') THEN
        CREATE ROLE crawler_app NOLOGIN;
    END IF;
END
$$;

-- crawler_readonly: read-only role for analytics, BI, and monitoring.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'crawler_readonly') THEN
        CREATE ROLE crawler_readonly NOLOGIN;
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Register Azure AD managed identities as PostgreSQL principals.
-- pgaadauth_create_principal(name, isAdmin, isGroupOrAppReg)
--   isAdmin = false  — do not grant pg_monitor / superuser
--   false           — this is a managed identity (not a group / app registration)
--
-- The call is skipped if the principal already exists (the function is
-- idempotent on Flexible Server — it raises a notice, not an error).
--
-- Replace the placeholder names with the display names of your managed
-- identities as they appear in Azure Entra ID.
-- ---------------------------------------------------------------------------

-- Crawler pods runtime identity → crawler_app permissions
SELECT pgaadauth_create_principal('<crawler-app-managed-identity>', false, false);
GRANT crawler_app TO "<crawler-app-managed-identity>";

-- Optional: read-only monitoring identity → crawler_readonly permissions
-- Uncomment and substitute if you have a dedicated readonly MI or user.
-- SELECT pgaadauth_create_principal('<crawler-readonly-identity>', false, false);
-- GRANT crawler_readonly TO "<crawler-readonly-identity>";


-- =============================================================================
-- TABLE: edge_records
-- Records each directed edge discovered during a crawl walk.
-- One row per (source → destination) observation; duplicates are intentional
-- in random-walk mode — multiple sources in the same layer may link to the
-- same destination and each observation is a distinct data point.
-- =============================================================================

CREATE TABLE IF NOT EXISTS edge_records (
    edge_id             SERIAL       PRIMARY KEY,
    destination_channel VARCHAR(64)  NOT NULL,
    source_channel      VARCHAR(64)  NOT NULL,
    walkback            BOOLEAN      NOT NULL,
    skipped             BOOLEAN      NOT NULL,
    discovery_time      TIMESTAMP    NOT NULL,
    crawl_id            VARCHAR(64)  NOT NULL,
    sequence_id         VARCHAR(36)  NOT NULL DEFAULT ''
    -- sequence_id: UUID shared across all edges in one uninterrupted forward
    -- walk chain (A→B→C). A walkback generates a fresh UUID for the new chain.
    -- Empty string means sequence tracking not used for this row.
);

-- Common query: fetch all edges for a crawl session
CREATE INDEX IF NOT EXISTS idx_edge_records_crawl_id
    ON edge_records (crawl_id);

-- Common query: find all outbound edges from a source channel
CREATE INDEX IF NOT EXISTS idx_edge_records_source_channel
    ON edge_records (source_channel);

-- Chain analysis: look up all edges belonging to one sequence
CREATE INDEX IF NOT EXISTS idx_edge_records_sequence_id
    ON edge_records (sequence_id)
    WHERE sequence_id <> '';

-- Time-range analytics
CREATE INDEX IF NOT EXISTS idx_edge_records_discovery_time
    ON edge_records (discovery_time);

-- Composite: per-crawl source lookup (covers most runtime query patterns)
CREATE INDEX IF NOT EXISTS idx_edge_records_crawl_source
    ON edge_records (crawl_id, source_channel);

-- crawler_app: full DML + SERIAL sequence access (INSERT calls nextval())
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE edge_records      TO crawler_app;
GRANT USAGE, SELECT ON SEQUENCE edge_records_edge_id_seq        TO crawler_app;

-- crawler_readonly: SELECT only, no sequence access needed
GRANT SELECT ON TABLE edge_records                              TO crawler_readonly;


-- =============================================================================
-- TABLE: page_buffer
-- Transient queue of pages to process in the next BFS/random-walk layer.
-- Scoped per pod via crawl_id — each pod only reads/writes its own rows.
-- Rows are deleted after a layer completes; this table should stay small.
-- =============================================================================

CREATE TABLE IF NOT EXISTS page_buffer (
    page_id     VARCHAR(36)  PRIMARY KEY,     -- UUID
    parent_id   VARCHAR(36)  NOT NULL,        -- UUID of parent page
    depth       INTEGER      NOT NULL,
    url         VARCHAR(64)  NOT NULL,        -- channel username
    crawl_id    VARCHAR(64)  NOT NULL,
    sequence_id VARCHAR(36)  NOT NULL DEFAULT ''
);

-- All runtime queries filter on crawl_id (pod isolation)
CREATE INDEX IF NOT EXISTS idx_page_buffer_crawl_id
    ON page_buffer (crawl_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE page_buffer      TO crawler_app;
GRANT SELECT ON TABLE page_buffer                              TO crawler_readonly;


-- =============================================================================
-- TABLE: seed_channels
-- Canonical list of seed channels for random-walk crawls (~500k rows).
-- Dual-purpose: initial seed pool + chat ID cache (avoids redundant TDLib
-- SearchPublicChat RPCs) + last-crawl watermark (delta-fetch on revisit).
-- =============================================================================

CREATE TABLE IF NOT EXISTS seed_channels (
    channel_username  VARCHAR(64)  PRIMARY KEY,
    chat_id           BIGINT,                   -- cached TDLib chat ID; NULL = not yet resolved
    last_crawled_at   TIMESTAMP,                -- NULL = never crawled; set by MarkChannelCrawled()
    member_count      INTEGER,
    inserted_at       TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Seed selection: quickly find channels never crawled (NULL first)
CREATE INDEX IF NOT EXISTS idx_seed_channels_last_crawled
    ON seed_channels (last_crawled_at NULLS FIRST);

-- Partial index for the common "pick uncrawled seeds" pattern
CREATE INDEX IF NOT EXISTS idx_seed_channels_uncrawled
    ON seed_channels (inserted_at)
    WHERE last_crawled_at IS NULL;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE seed_channels     TO crawler_app;
GRANT SELECT ON TABLE seed_channels                             TO crawler_readonly;


-- Seed table from csv
-- \COPY seed_channels (channel_username) FROM '<absolute_path_to_seed_channels>' WITH (FORMAT csv, HEADER true);

-- =============================================================================
-- TABLE: invalid_channels
-- Shared cache of channel usernames that failed SearchPublicChat validation.
-- TTL of 30 days is enforced in application logic (IsInvalidChannel checks
-- invalidated_at). Shared across all pods to reduce redundant API calls.
-- =============================================================================

CREATE TABLE IF NOT EXISTS invalid_channels (
    channel_username  VARCHAR(64)  PRIMARY KEY,
    reason            VARCHAR(64)  NOT NULL DEFAULT '',   -- e.g. "not_found", "not_supergroup"
    invalidated_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- TTL query: load only non-expired rows on startup
CREATE INDEX IF NOT EXISTS idx_invalid_channels_invalidated_at
    ON invalid_channels (invalidated_at);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE invalid_channels  TO crawler_app;
GRANT SELECT ON TABLE invalid_channels                          TO crawler_readonly;
