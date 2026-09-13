CREATE TYPE scan_state AS ENUM ('running','evaluating','complete','failed');
CREATE TYPE task_state AS ENUM ('pending','done','failed');
CREATE TYPE severity   AS ENUM ('critical','high','medium','low','info');
CREATE TYPE triage     AS ENUM ('open','acknowledged','suppressed','resolved');
CREATE TYPE cred_kind  AS ENUM ('password','certificate','federated');

CREATE TABLE scan_runs (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   text        NOT NULL,
    state       scan_state  NOT NULL DEFAULT 'running',
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    stats       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT scan_runs_finished_iff_terminal CHECK (
        (state IN ('complete','failed')) = (finished_at IS NOT NULL))
);
CREATE INDEX scan_runs_tenant_started_idx ON scan_runs (tenant_id, started_at DESC);

CREATE TABLE scan_tasks (
    scan_run_id uuid       NOT NULL REFERENCES scan_runs ON DELETE CASCADE,
    task_key    text       NOT NULL,
    state       task_state NOT NULL DEFAULT 'pending',
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scan_run_id, task_key)
);
CREATE INDEX scan_tasks_pending_idx ON scan_tasks (scan_run_id)
    WHERE state = 'pending';

CREATE TABLE identities (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    source        text        NOT NULL DEFAULT 'entra',
    external_id   text        NOT NULL,
    kind          text        NOT NULL,
    display_name  text        NOT NULL,
    app_id        text,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, external_id)
);

CREATE TABLE identity_snapshots (
    scan_run_id     uuid    NOT NULL REFERENCES scan_runs ON DELETE CASCADE,
    identity_id     uuid    NOT NULL REFERENCES identities,
    account_enabled boolean NOT NULL,
    sign_in_at      timestamptz,
    owner_count     int     NOT NULL DEFAULT 0,
    raw             jsonb   NOT NULL,
    PRIMARY KEY (scan_run_id, identity_id)
);

CREATE TABLE credentials (
    scan_run_id  uuid      NOT NULL REFERENCES scan_runs ON DELETE CASCADE,
    identity_id  uuid      NOT NULL REFERENCES identities,
    key_id       text      NOT NULL,
    kind         cred_kind NOT NULL,
    display_name text,
    not_before   timestamptz,
    not_after    timestamptz,
    subject      text,
    issuer       text,
    PRIMARY KEY (scan_run_id, identity_id, key_id),
    CONSTRAINT credentials_expiry_shape CHECK (
        (kind = 'federated') OR (not_after IS NOT NULL))
);
CREATE INDEX credentials_expiry_idx ON credentials (scan_run_id, not_after)
    WHERE not_after IS NOT NULL;

CREATE TABLE privileges (
    scan_run_id uuid NOT NULL REFERENCES scan_runs ON DELETE CASCADE,
    identity_id uuid NOT NULL REFERENCES identities,
    system      text NOT NULL,
    role        text NOT NULL,
    scope       text NOT NULL,
    PRIMARY KEY (scan_run_id, identity_id, system, role, scope)
);

CREATE TABLE declared_identities (
    scan_run_id   uuid NOT NULL REFERENCES scan_runs ON DELETE CASCADE,
    source        text NOT NULL DEFAULT 'entra',
    external_id   text NOT NULL,
    resource_addr text NOT NULL,
    PRIMARY KEY (scan_run_id, source, external_id)
);

CREATE TABLE findings (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    scan_run_id uuid        NOT NULL REFERENCES scan_runs ON DELETE CASCADE,
    identity_id uuid        NOT NULL REFERENCES identities,
    rule_id     text        NOT NULL,
    severity    severity    NOT NULL,
    fingerprint text        NOT NULL,
    evidence    jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (scan_run_id, fingerprint)
);
CREATE INDEX findings_fingerprint_idx ON findings (fingerprint, created_at DESC);

CREATE TABLE finding_states (
    fingerprint text   PRIMARY KEY,
    state       triage NOT NULL DEFAULT 'open',
    note        text,
    actor       text   NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT finding_states_note_required CHECK (
        state <> 'suppressed' OR note IS NOT NULL)
);
