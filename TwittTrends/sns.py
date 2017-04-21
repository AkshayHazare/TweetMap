import boto
import boto.exception
import boto.sns
import pprint
import re


def send_push(device_id, body):

    sns = boto.sns.SNSConnection(
        aws_access_key_id='AKIAJD27C4BSAXDDCOIQ',
        aws_secret_access_key='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',
        region="us-west-2")

    try:
        endpoint_response = sns.create_platform_endpoint(
            platform_application_arn='arn:aws:sns:us-west-2:150887107369:TweetMap',
            token=device_id,
        )
        endpoint_arn = endpoint_response['CreatePlatformEndpointResponse']['CreatePlatformEndpointResult'][
            'EndpointArn']
    except boto.exception.BotoServerError, err:
        # http://stackoverflow.com/questions/22227262/aws-boto-sns-get-endpoint-arn-by-device-token
        result_re = re.compile(r'Endpoint(.*)already', re.IGNORECASE)
        result = result_re.search(err.message)
        if result:
            endpoint_arn = result.group(0).replace('Endpoint ', '').replace(' already', '')
        else:
            raise

    print "ARN:", endpoint_arn

    publish_result = sns.publish(
        target_arn=endpoint_arn,
        message=body,
    )
    print "PUBLISH"
    pprint.pprint(publish_result)