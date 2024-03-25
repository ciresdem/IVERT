/*
 This is a SQLite database schema definition for the "ivert_jobs" database.

 The "ivert_jobs" database contains a record of all jobs (and all files) processed by IVERT on the AWS server.

 This record lives both on the local S3 instance, but also in the "EXPORT/" bucket prefix to be read externally.
 The databse is updated every time a change is made to a job, including kicking off a new job, or processing a file.

 A subset of this database will be stored in the "ivert_jobs_latest" database, which will contain a much-smaller
 subset of only the latest jobs.
 */

CREATE TABLE IF NOT EXISTS ivert_jobs (
    job_id          INTEGER         NOT NULL    CHECK (job_id > 200000000000 AND job_id < 300000000000),
    -- The job_id should be a 12-digit number, in the general format YYYYMMDDNNNN, so the year (YYYY) should be
    -- something between 2000 and 3000.
    username        VARCHAR(128)    NOT NULL,
    user_email      VARCHAR(256)                CHECK (user_email like '%@%'), -- email should contain '@'
    import_prefix   VARCHAR(1024)   NOT NULL,
    import_bucket   VARCHAR(64)     NOT NULL,
    export_prefix   VARCHAR(1024),
    export_bucket   VARCHAR(64),
    -- some jobs may have no export needed. In that case, the 'export_prefix' and 'export_bucket' remain null.
    command         VARCHAR(64)     NOT NULL,
    -- command will be the primary command being run, such as 'validate', '
    configfile      VARCHAR(128)    NOT NULL,
    processing_dir  VARCHAR(512),
    job_pid         INTEGER         NOT NULL,
    status          VARCHAR(16)     NOT NULL    DEFAULT 'initiated',
        -- Possible values for status are: "initiated", "started", "running", "complete", "error"
    PRIMARY KEY (job_id, username)
);

CREATE TABLE IF NOT EXISTS files (
    job_id              INTEGER     NOT NULL,
    username            VARCHAR(128)    NOT NULL,
    filename            VARCHAR(256)    NOT NULL,
    import_or_export    INTEGER     NOT NULL    DEFAULT 0    CHECK(import_or_export in (0,1)),
    -- 0 means the file was imported for that job.
    -- 1 means it was exported from that job.
    size_bytes          INTEGER     NOT NULL    CHECK(size_bytes >= 0),
    md5                 VARCHAR(32)             CHECK (length (md5) == 32),
    FOREIGN KEY(job_id, username)
        REFERENCES ivert_jobs(job_id, username)
        ON DELETE CASCADE -- NOTE: In order to enforce this foreign, we must run "PRAGMA foreign_keys = ON;" whenever
                          -- we connect to the database. Foreign key constraints are disabled by default.
);