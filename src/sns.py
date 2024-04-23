"""sns.py -- Code for pushing messages to AWS SNS topics."""

import boto3
import typing

import utils.configfile

ivert_config = utils.configfile.config()


def send_sns_message(subject: str,
                     message: str,
                     job_id: typing.Union[str, int],
                     username: str,
                     job_message_num: int = 0) -> None:
    """Send a message to an AWS SNS topic."""
    client = boto3.client('sns')

    topic_arn = ivert_config.sns_arn
    assert topic_arn is not None

    # Get a message ID, so we can associate it with the job.
    # For the deduplication of job messages, the message_id is the username, job ID and the message number.
    # For the grouping of job messages, the group_id is the username and job ID.
    message_id = "{0}-{1}-{2}".format(username, job_id, job_message_num)
    group_id = "{0}-{1}".format(username, job_id)

    # Send the message.
    reply = client.publish(TopicArn=topic_arn,
                           Subject=subject,
                           Message=message,
                           MessageDeduplicationId=message_id,
                           MessageGroupId=group_id)

    print(type(reply))
    print(reply)
    return reply


if __name__ == "__main__":
    send_sns_message("SNS Test email subject",
                     "SNS Test email body",
                     202404160000,
                     "michael.macferrin",
                     0)
