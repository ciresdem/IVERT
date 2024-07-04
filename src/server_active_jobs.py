"""A utility for returning all active IVERT jobs on the server."""

import psutil
import typing

import utils.configfile
import jobs_database


def get_active_ivert_jobs(update_inactive_job_statuses: bool = False) -> typing.List[psutil.Process]:
    """Return a list of all active IVERT processing currently running on the server."""
    jobs_db = jobs_database.JobsDatabaseServer()
    jobs_db.download_from_s3(only_if_newer=True)
    df = jobs_db.read_table_as_pandas_df("jobs")
    df = df[df["status"].isin(["started", "running"])]

    database_updated = False
    active_ivert_jobs = []
    for i, (job_pid, username, job_num) in df[["job_pid", "username", "job_id"]].iterrows():
        if job_pid in psutil.pids():
            try:
                proc = psutil.Process(job_pid)

                if "python3" in proc.name():
                    # DON'T include the maintain_server_manager.py process, by chance.
                    if proc.is_running() and "maintain_server_manager.py" not in proc.cmdline():
                        active_ivert_jobs.append(proc)
                    elif update_inactive_job_statuses:
                        jobs_db.update_job_status(username, job_num, "unknown",
                                                  increment_vnum=False, upload_to_s3=False)
                        database_updated = True

                elif update_inactive_job_statuses:
                    jobs_db.update_job_status(username, job_num, "unknown",
                                              increment_vnum=False, upload_to_s3=False)
                    database_updated = True

            except psutil.NoSuchProcess:
                continue

        elif update_inactive_job_statuses:
            jobs_db.update_job_status(username, job_num, "unknown",
                                      increment_vnum=False, upload_to_s3=False)
            database_updated = True

    if database_updated:
        jobs_db.increment_vnumber()
        jobs_db.upload_to_s3()

    return active_ivert_jobs


if __name__ == "__main__":
    for proc in get_active_ivert_jobs(update_inactive_job_statuses=True):
        print(proc, proc.cmdline())
