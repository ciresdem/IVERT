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
    -- something between 2000 and 3000. Job numbers should only increase, so max(job_id) should return the last
    -- job number run (and let you formulate the next job number).
    --
    -- Since there can exist "race" conditions where two different users submit jobs at the same time and are given the
    -- exact same YYYYMMDDNNNN job-number, the (job_id, username) pair is treated as a unique key in this table.
    -- (See below for the "PRIMARY KEY" definition.)
    username        VARCHAR(128)    NOT NULL,

    -- The buckets on the import (trusted) directory where the files will be found.
    import_prefix   VARCHAR(1024)   NOT NULL,

    -- some jobs may have no export needed. In that case, the 'export_prefix' and 'export_bucket' remain null.
    export_prefix   VARCHAR(1024),

    -- command will be the primary command being run: 'validate', 'update', 'import'.
    command         VARCHAR(16)     NOT NULL,

    -- the full command-line options of the command, as listed in the configfile.
    command_args    VARCHAR(2048)   NOT NULL,
    -- every job should come with a config .ini file. The name of it goes here.
    configfile      VARCHAR(128)    NOT NULL,

    -- Many jobs generate a logfile with outputs that the user can download. It is placed in the export_bucket under
    -- the export_prefix folder.
    logfile         VARCHAR(128)    NOT NULL,

    -- The directory into which the input files will be locally downloaded.
    -- This will typically be [ivert_basedir]/ivert_data/jobs/[command]/[username]/[job_id]/
    input_dir_local VARCHAR(1024),
    -- The directory into which the output files will be written and placed.
    -- This will typically be a subdirectory of the input_dir_local called 'outputs'.
    output_dir_local VARCHAR(1024),

    -- The process ID # on the machine that is processing this job. The parent process may spin off multiple
    -- sub-processes that are not captured here. This allows IVERT to check on the existence of any jobs that may have
    -- quit or been killed, or to see if they're still running.
    job_pid         INTEGER,

    -- Possible values for status are: "initialized", "started", "running", "complete", "error", "unknown"
    status          VARCHAR(16)     NOT NULL    DEFAULT 'unknown'
                            CHECK (status in ('started', 'running', 'complete', 'error', 'killed', 'unknown')),

    -- Job_id and username are a unique primary key.
    -- Job_id will (usually) be unique by itself, but two different users could submit jobs at the same time,
    -- which may end up having the same job_id, so we pair it with the username to ensure uniqueness.
    -- If a single user submits two jobs with the same ID (which the front-end script will prevent but could
    -- feasibly happen if they did it manually), those will actually be written to the same folder, be treated as the
    -- exact same job, and (most likely) only the files that are listed in the first config.ini file uploaded will
    -- be processed. The second (identical) job will be ignored and not processed.
    PRIMARY KEY (job_id, username)
);

CREATE TABLE IF NOT EXISTS ivert_files (
    -- Job ID and username are used to link each file to a job entered above.
    -- FOREIGN KEY constraints are listed below to ensure this link.
    job_id              INTEGER         NOT NULL,
    username            VARCHAR(128)    NOT NULL,

    -- Name of the file. This is just the "basename." The import, export, and local processing directories can be
    -- retrieved from the "ivert_jobs" table for the job associated with this file.
    filename            VARCHAR(256)    NOT NULL,

    -- Both imported files and exported files are tracked here. This flag indicates which each file was.
    -- 0 means the file was imported for that job.
    -- 1 means it was exported from that job.
    -- 2 means the file was imported and will also be exported.
    import_or_export    INTEGER         NOT NULL    DEFAULT 0   CHECK(import_or_export in (0,1,2)),

    -- Size of the file in bytes.
    size_bytes          INTEGER         NOT NULL    CHECK (size_bytes >= 0),

    -- md5sum of the file in 32-byte character string.
    md5                 VARCHAR(32)                 CHECK (length (md5) == 32),

    -- Status of the individual file.
    status              VARCHAR(16)     NOT NULL    DEFAULT 'unknown'
                            CHECK (status in ('downloaded', 'timeout', 'processing', 'processed',
                                              'uploaded', 'error', 'quarantined', 'unknown', 'unprocessed')),

    -- The job_id, username, filename tuple should be unique.
    PRIMARY KEY (job_id, username, filename)

    -- The foreign key should map back to a job in the ivert_jobs table.
    FOREIGN KEY(job_id, username)
        REFERENCES ivert_jobs(job_id, username)
        ON DELETE CASCADE -- NOTE: In order to enforce this foreign key, we must run "PRAGMA foreign_keys = ON;"
                          -- whenever we connect to the database. Foreign key constraints are disabled by default in
                          -- python's sqlite3 library.
                          -- "ON DELETE CASCADE" means that if a record is deleted from the ivert_jobs table, all file
                          -- records linked to that job in this table should also be deleted automatically.
);

CREATE TABLE IF NOT EXISTS sns_subscriptions (
-- Table keeping track of any SNS (Simple Notification Service) subscription requests for new users.
-- These are typically updated using the "update" command with a ivert_user_config.ini file attached.
-- This does not track whether a user later unsubscribed. It just creates a record of all the subscription requests
-- that IVERT has processed, regardless of their current status.
    username        VARCHAR(128)    NOT NULL,
    user_email      VARCHAR(256)    NOT NULL     CHECK (user_email like '%@%'), -- email should contain '@'
    topic_arn       VARCHAR(2048)   NOT NULL     CHECK(length(topic_arn) >= 20),
    sns_filter_string VARCHAR(1024), -- Typically we're filtering by a single username. But it can be a (string-converted)
                                    -- list of usernames as well. Null here implies no filtering was applied.
    sns_arn         VARCHAR(2048) -- The return value ARN provided when the subscription was executed.
);

CREATE TABLE IF NOT EXISTS sns_messages (
-- Table to keep track of all SNS messages that we send out.
    job_id          INTEGER         NOT NULL,
    username        VARCHAR(128)    NOT NULL,
    subject         VARCHAR(256)    NOT NULL,
    -- message         VARCHAR(8192)   NOT NULL,
    -- We've decided *not* to keep full-text copies of all the messsages.
    -- This could blow up the database size pretty quickly.
    response        VARCHAR(2048),  -- The full response returned by the call to aws.sns.publish in JSON format. This
                                    -- includes the date it was sent, the size, and the MessageID.
    FOREIGN KEY(job_id, username)
        REFERENCES ivert_jobs(job_id, username)
        ON DELETE CASCADE -- Links to the uniqe (job_id, username) pair in ivert_jobs. More than one message may be
                          -- sent from a single job.
);

CREATE TABLE IF NOT EXISTS vnumber (
    -- A single value holding the current "version number" (vnum) of the database.
    -- This value is incremented every time the database is updated & uploaded, and is also set as a metadata
    -- parameter in the S3 key. This allows us to quickly see if we have the "most current" version of the database
    -- in hand.
    vnum INT NOT NULL,
    -- "enforcer" is a dummy field (set to zero) that does not allow us to insert another 'vnum'
    -- record into this table. If we do it'll try to create another "enforcer" value set to zero which will break
    -- the UNIQUE constraint. This helps ensure there is only one single record in this table.
    enforcer INT DEFAULT 0 NOT NULL CHECK (enforcer == 0),
    UNIQUE (enforcer)
);

-- Insert the one row into the "vnumber" table, initialized to version 0.
INSERT INTO vnumber (vnum) VALUES (0);
