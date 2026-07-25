/*
 * Dev/test database bootstrap. Runs once on an empty volume; `bb db:reset`
 * drops the volume so it reruns.
 *
 *   dbos / dbos    — dev account, used by the REPL and the test suite
 *   dbos_clj_dev   — scratch database for REPL experiments
 *   dbos_clj_test  — throwaway database for the :integration suite
 *
 * The account OWNS both databases because DBOS creates and migrates its own
 * schema on launch, which needs DDL rights.
 *
 * Duplicated in .env (this file can't read env vars) — keep them in sync.
 */

CREATE ROLE dbos WITH LOGIN PASSWORD 'dbos' CREATEDB;

CREATE DATABASE dbos_clj_dev OWNER dbos;
CREATE DATABASE dbos_clj_test OWNER dbos;

-- PG15+ revokes CREATE on `public` from non-owners.
\connect dbos_clj_dev
ALTER SCHEMA public OWNER TO dbos;

\connect dbos_clj_test
ALTER SCHEMA public OWNER TO dbos;
