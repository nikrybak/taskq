CREATE TABLE jobs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}',
    priority         smallint    NOT NULL DEFAULT 0,
    state            text        NOT NULL DEFAULT 'queued',
    attempts         integer     NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL,
    run_at           timestamptz NOT NULL DEFAULT now(),
    idempotency_key  text,
    last_error       text,
    locked_by        text,
    lease_expires_at timestamptz,
    started_at       timestamptz,
    finished_at      timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT jobs_state_check
        CHECK (state IN ('queued', 'running', 'succeeded', 'dead', 'cancelled')),
    CONSTRAINT jobs_attempts_check
        CHECK (attempts >= 0 AND max_attempts >= 1),
    -- A running job always has a lease and nothing else does; the reaper
    -- relies on this to find abandoned work.
    CONSTRAINT jobs_lease_check
        CHECK ((state = 'running') = (lease_expires_at IS NOT NULL))
);

-- Serves the claim query: walk runnable jobs in dispatch order and stop at
-- the first unlocked rows. Partial, so finished jobs never bloat it.
CREATE INDEX jobs_claim_idx ON jobs (priority DESC, run_at, id) WHERE state = 'queued';

-- Serves the reaper looking for expired leases.
CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at) WHERE state = 'running';

-- Idempotency keys are optional but unique when present. Recurring jobs use
-- the same constraint to guarantee one job per schedule tick.
CREATE UNIQUE INDEX jobs_idempotency_key_idx ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Keyset pagination for GET /jobs filtered by state or type.
CREATE INDEX jobs_state_id_idx ON jobs (state, id DESC);
CREATE INDEX jobs_type_id_idx ON jobs (type, id DESC);
