CREATE TABLE jobs (
    id               uuid        PRIMARY KEY,
    queue            text        NOT NULL DEFAULT 'default',
    job_type         text        NOT NULL,
    payload_version  int         NOT NULL DEFAULT 1,
    payload          jsonb       NOT NULL,
    state            text        NOT NULL DEFAULT 'pending',
    attempt          int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL DEFAULT 20,
    run_at           timestamptz NOT NULL DEFAULT now(),
    claimed_at       timestamptz,
    claimed_by       text,
    heartbeat_at     timestamptz,
    last_error       text,
    last_error_class text,
    checkpoint       jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT jobs_state_valid CHECK (state IN ('pending','claimed','dead')),
    CONSTRAINT jobs_claim_coherent CHECK (
        (state = 'claimed') = (claimed_by IS NOT NULL)),
    CONSTRAINT jobs_attempt_sane CHECK (attempt >= 0 AND attempt <= max_attempts)
) WITH (fillfactor = 85);

CREATE INDEX jobs_claim_idx     ON jobs (queue, run_at, id) WHERE state = 'pending';
CREATE INDEX jobs_heartbeat_idx ON jobs (heartbeat_at)      WHERE state = 'claimed';

CREATE TABLE jobs_archive (LIKE jobs INCLUDING DEFAULTS,
    archived_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX jobs_archive_archived_idx ON jobs_archive (archived_at);