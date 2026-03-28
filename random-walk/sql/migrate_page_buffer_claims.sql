-- Migration: Add claim columns to page_buffer for multi-pod page claiming.
-- Safe to run on an existing table with data — adds columns with defaults,
-- creates indexes with IF NOT EXISTS.
--
-- Run manually: psql -f sql/migrate_page_buffer_claims.sql

BEGIN;

-- 1. Add claim tracking columns (no-op if they already exist).
ALTER TABLE page_buffer ADD COLUMN IF NOT EXISTS claimed_by  VARCHAR(128) NOT NULL DEFAULT '';
ALTER TABLE page_buffer ADD COLUMN IF NOT EXISTS claimed_at  TIMESTAMPTZ;

-- 2. Idempotent seeding: prevents duplicate seeds when multiple pods race.
CREATE UNIQUE INDEX IF NOT EXISTS idx_page_buffer_crawl_url
    ON page_buffer (crawl_id, url);

-- 3. Fast path for ClaimPages: find unclaimed rows ordered by depth.
CREATE INDEX IF NOT EXISTS idx_page_buffer_claimable
    ON page_buffer (crawl_id, depth) WHERE claimed_by = '';

COMMIT;
