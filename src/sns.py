"""sns.py -- Code for pushing messages to AWS SNS topics."""

import boto3
import typing

import utils.configfile

ivert_config = utils.configfile.config()


def send_sns_message(subject: str,
                     message: str,
                     job_id: typing.Union[str, int],
                     username: str) -> dict:
    """Send a message to an AWS SNS topic.

    Args:
        subject (str): The subject of the message.
        message (str): The body of the message.
        job_id (typing.Union[str, int]): The 12-digit numerical ID of the job.
        username (str): The username of the user who submitted the job.

    Returns:
        dict: The response attributues from the SNS API.
    """
    client = boto3.client('sns')

    topic_arn = ivert_config.sns_arn
    assert topic_arn is not None

    # We could send "job_id" as a "Number" datatype but they max it out at 10^9 and our job_ids are 12 digits.
    msg_attributes = {{"Name": "job_id", "Type": "String", "Value": str(job_id)},
                      {"Name": "username", "Type": "String", "Value": username},
                      }

    # Send the message.
    reply = client.publish(TopicArn=topic_arn,
                           Subject=subject,
                           Message=message,
                           MessageAttributes=msg_attributes)

    return reply


if __name__ == "__main__":
    username = "michael.macferrin"
    job_id = 202404160000
    subject = f"IVERT: Job {username}_{job_id} has been created."

    message = f"""This is an automated message from the ICESat-2 Validation of Elevations Reporting Tool (IVERT). Do not reply to this message.

Your job "{username}_{job_id}" has been created and is being started. You can monitor the status of your job at any time by running "ivert status {username}_{job_id}" at the command line.

The following options are assigned to this job:
[Insert stuff here for message options.]

If you wish to cancel your job, run "ivert kill {username}_{job_id}" at the command line and it will be terminated when the EC2 receives the notification. All files uploaded with the job will be deleted.

[Insert this bit only if results will be generated.]
You will get another email when the job is complete and your results are ready to download."""

    send_sns_message(subject,
                     message,
                     job_id,
                     username)
