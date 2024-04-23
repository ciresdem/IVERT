"""sns.py -- Code for pushing messages to AWS SNS topics."""

import argparse
import boto3
import typing

import utils.configfile
import utils.is_email

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
    msg_attributes = {"job_id" : {"DataType": "String", "StringValue": str(job_id)},
                      "username": {"DataType": "String", "StringValue": username},
                      }

    # Send the message.
    reply = client.publish(TopicArn=topic_arn,
                           Subject=subject,
                           Message=message,
                           MessageAttributes=msg_attributes)

    return reply


def subscribe(email: str,
              username_filter: typing.Union[str, list[str], None] = None) -> None:
    """Subscribe an email address to the IVERT AWS SNS topic.

    Args:
        email (str): The email address to subscribe.
        username_filter (typing.Union[str, list[str], None]): The username or list of usernames to filter on.
                    Defaults to None, which applies no filters (you'll get all the messages).

    Returns:
        None
    """
    client = boto3.client('sns')

    if not utils.is_email.is_email(email):
        raise ValueError(f"'{email}' is not a valid email address.")

    # Get the SNS topic ARN from the ivert_config object (which fetches it from ivert_setup/setup/paths.sh).
    topic_arn = ivert_config.sns_arn
    assert topic_arn is not None

    if type(username_filter) is str:
        filter_policy = {"username": [username_filter]}
    elif type(username_filter) is list:
        filter_policy = {"username": username_filter}
    else:
        filter_policy = None

    # Send the message.
    reply = client.subscribe(TopicArn=topic_arn,
                             Protocol="email",
                             Endpoint=email,
                             ReturnSubscriptionArn=True,
                             Attributes={"FilterPolicy": filter_policy,
                                         "FilterPolicyScope": "MessageAttributes"} if filter_policy else None,
                             )

    return reply


def define_and_parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send a message to an AWS SNS topic.")
    parser.add_argument("-s", "--subject", dest="subject", type=str, required=True,
                        help="The subject of the message.")
    parser.add_argument("-m", "--message", dest="message", type=str, required=True,
                        help="The body of the message.")
    parser.add_argument("-j", "--job_id", dest="job_id", type=str, required=True,
                        help="The 12-digit numerical ID of the job.")
    parser.add_argument("-u", "--username", dest="username", type=str, required=True,
                        help="The username of the user who submitted the job.")

    return parser.parse_args()


if __name__ == "__main__":
    args = define_and_parse_args()

    subject = f"IVERT: Job \"{args.username}_{args.job_id}\" has been created."

    message = f"""This is an automated message from the ICESat-2 Validation of Elevations Reporting Tool (IVERT).
Do not reply to this message.

Your job "{args.username}_{args.job_id}" has been created and is being started.
You can monitor the status of your job at any time by running "ivert status {args.username}_{args.job_id}" at the command line.

The following options are assigned to this job:
[Insert stuff here for message options.]

If you wish to cancel your job, run "ivert kill {args.username}_{args.job_id}" at the command line and it will be terminated when the EC2 receives the notification. All files uploaded with the job will be deleted.

[Insert this bit only if results will be generated.]
You will get another email when the job is complete and your results are ready to download."""

    response = send_sns_message(subject,
                                message,
                                args.job_id,
                                args.username)

    print(response)