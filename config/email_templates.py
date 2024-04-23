email_intro = """
Hello,
This is an automated email from the ICESat-2 Validation of Elevations Reporting Tool (IVERT).
Do not reply to this message.
"""
# Subject line for when a job is submitted and detected on the EC2.
# 0: username,
# 1: job_id
subject_template_job_submitted = "IVERT: Job \"{0}_{1}\" has been created"

# Email template to send out when a job is submitted and detected on the EC2.
# 0: username,
# 1: job_id
# 2: full command being processed
email_template_job_submitted = email_intro + """
Your IVERT job "{0}_{1}" has been created and queued for processing.

The following IVERT command is being run on the EC2 server:
{2}

You can monitor the status of your job at any time by running "ivert status {0}_{1}" at the command line.

If you wish to cancel this job, run "ivert kill {0}_{1}" at the command line and it will be terminated when the EC2 receives the notification. All files uploaded with the job will be deleted, along with any output files.

You will get another email when the job is complete and results (if any) are ready to download."""

# Subject line for when a job is finished.
# 0: username,
# 1: job_id
# 2: "completed successfully", "completed with partial success", or "terminated unexpectedly"
subject_template_job_completed = "IVERT: Job \"{0}_{1}\" has {2}"

# Email template to send out if the job is finished without any exports.
# 0: username,
# 1: job_id
# 2: "completed successfully", "completed with partial success", or "terminated early"
# 3: number of input files processed (not including the config file)
# 4: number of input files processed successfully.
# 5: number of input files processed unsuccessfully (errors, etc)
email_template_job_finished_without_exports = """
Your IVERT job "{0}_{1}" has {2}.

It processed {3} input files ({4} successful, {5} unsuccessful).
"""

# An addendum to add if any files were not completed successfully, or if the job itself was unsuccessful or
# terminated abruptly.
# 0: username,
# 1: job_id
email_template_job_finished_unsuccessful_addendum = """
If the job was unsuccessful or any files were not completed successfully, you may download a detailed logfile by running "ivert download {0}_{1}" at the command line and send it to the IVERT developers to debug and fix the issue.
"""

# An email addendum to add if any files were exported.
# 0: username,
# 1: job_id
# 6: number of output files exported
# 7: total size of exported files (in a human-readable format)
email_template_job_finished_with_exports = email_template_job_finished_without_exports + """
{6} output files have been exported with a total files size of {7}. To download them, run "ivert download {0}_{1}" at the command line.

IVERT result files have a lifecycle of 7 days and may be deleted from the server after that time.
"""


email_field_definitions = {"submitted": {0: "username", 1: "job_id", 2: "full_command"},
                "finished": {0: "username", 1: "job_id", 2: "status", 3: "input_files_successful",
                             4: "input_files_with_results", 5: "input_files_unsuccessful", 6: "output_files_exported"}}

subject_field_definitions = {0: "username", 1: "job_id", 2: "successfully_or_unsuccessfully"}