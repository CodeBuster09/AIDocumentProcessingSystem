CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ─────────────────────────────────────────────── users

CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Case-insensitive uniqueness enforced by the database, not by remembering
-- to lowercase in application code.
CREATE UNIQUE INDEX ux_users_email ON users (lower(email));

-- Phase 1 has no auth. This user owns everything until phase 2 adds JWT.
INSERT INTO users (id, email, password_hash)
VALUES ('00000000-0000-0000-0000-000000000001', 'dev@localhost', 'not-a-real-hash');

-- ─────────────────────────────────────────────── documents

CREATE TYPE doc_status AS ENUM
    ('UPLOADED', 'PROCESSING', 'COMPLETED', 'FAILED');

CREATE TABLE documents (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID NOT NULL REFERENCES users(id),

    filename         TEXT     NOT NULL,   -- what the user called it
    storage_bucket   TEXT     NOT NULL,
    storage_key      TEXT     NOT NULL,   -- {userID}/{uuid}.pdf — never a URL
    content_type     TEXT     NOT NULL,
    file_size        BIGINT   NOT NULL,
    checksum_sha256  CHAR(64) NOT NULL,
    page_count       INT,

    status           doc_status NOT NULL DEFAULT 'UPLOADED',
    summary          TEXT,
    failure_code     TEXT,                -- machine-readable: ENCRYPTED_PDF
    failure_detail   TEXT,                -- human-readable

    -- Which worker owns this document, and until when. Unused until phase 3,
    -- but the column belongs to the document's identity, not the worker's.
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at       TIMESTAMPTZ
);

CREATE INDEX ix_docs_user ON documents (user_id, created_at DESC)
    WHERE deleted_at IS NULL;

-- Lets the phase-4 sweeper find stranded work without a sequential scan.
CREATE INDEX ix_docs_stranded ON documents (status, updated_at)
    WHERE status IN ('UPLOADED', 'PROCESSING');

-- The same PDF uploaded twice reuses the existing document and skips the
-- entire pipeline. Cheapest cost saving in the system.
CREATE UNIQUE INDEX ux_docs_dedupe ON documents (user_id, checksum_sha256)
    WHERE deleted_at IS NULL;

-- ─────────────────────────────────────────────── chunks

CREATE TABLE document_chunks (
    id          BIGSERIAL PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    chunk_index INT  NOT NULL,
    content     TEXT NOT NULL,

    page_from   INT  NOT NULL,   -- citations are impossible without these
    page_to     INT  NOT NULL,
    token_count INT  NOT NULL,

    embedding   vector(768),     -- ollama nomic-embed-text
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Reprocessing a document cannot produce duplicate chunks.
    CONSTRAINT ux_chunk UNIQUE (document_id, chunk_index)
);

CREATE INDEX ix_chunks_doc ON document_chunks (document_id);

-- Deliberately no HNSW index yet. A few hundred vectors per document scans
-- fast, and an approximate index combined with a narrow WHERE filter loses
-- recall silently. Add it in phase 6 only if a benchmark justifies it.

-- ─────────────────────────────────────────────── jobs

CREATE TABLE jobs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id   UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    job_type      TEXT NOT NULL,           -- PROCESS_DOCUMENT
    status        TEXT NOT NULL,           -- PENDING RUNNING SUCCEEDED FAILED DEAD
    stage         TEXT,                    -- resumption checkpoint
    attempts      INT  NOT NULL DEFAULT 0,
    max_attempts  INT  NOT NULL DEFAULT 5,
    last_error    TEXT,
    next_retry_at TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ux_job UNIQUE (document_id, job_type)
);

CREATE INDEX ix_jobs_pending ON jobs (status, created_at)
    WHERE status = 'PENDING';

-- ─────────────────────────────────────────────── questions

CREATE TABLE questions (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id       UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    user_id           UUID NOT NULL REFERENCES users(id),

    question          TEXT NOT NULL,
    question_hash     CHAR(64) NOT NULL,   -- sha256(lowercased, trimmed)
    answer            TEXT NOT NULL,
    citations         JSONB NOT NULL,      -- [{chunkId, page, snippet}]

    model             TEXT NOT NULL,
    prompt_tokens     INT,
    completion_tokens INT,
    latency_ms        INT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_q_cache ON questions (document_id, question_hash);
