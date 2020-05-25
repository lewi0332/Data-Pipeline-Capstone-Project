import boto3
import json
import datetime
import urllib
import urllib3
import logging
from pprint import pprint
from requests_aws4auth import AWS4Auth
import requests
import botocore
from io import BytesIO
import re


"""
Can Override the global variables using Lambda Environment Parameters
"""
globalVars = {}
globalVars['Owner'] = "Derrick"
globalVars['Environment'] = "Dev"
globalVars['awsRegion'] = "us-east-1"
globalVars['tagName'] = "serverless-s3-to-es-stat-ingester"
globalVars['service'] = "es"
globalVars['esIndexPrefix'] = "/instagram_graph_users/"
globalVars['esIndexDocType'] = "_doc"
globalVars['esHosts'] = {
    'prod': 'future ES store',
    'aws': 'https://search-social-system-kkehzvprsvgkfisnfulapobkpm.us-east-1.es.amazonaws.com'
}

# Initialize Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def indexDocElement(es_Url, awsauth, docData):
    """
    Loads completed document to Elasticsearch index.

    PARAMS:
    es_url - Elasticsearch Url for PUT requests
    awsauth - AWS credentials for Elasticsearch
    docData - formated dict like object to update elasticsearch record. 
    """
    try:
        headers = {"Content-Type": "application/json"}
        resp = requests.put(es_Url, auth=awsauth,
                            headers=headers, json=docData)
        print(resp.content)
        if resp.status_code == 201:
            logger.info('INFO: Successfully created element into ES')
        elif resp.status_code == 200:
            logger.info('INFO: Successfully updated element into ES')
        else:
            logger.error(f'FAILURE: Unable to index element {resp.content}')
            raise
    except Exception as e:
        logger.error(f'ERROR: {str(e)}')
        logger.error(f"ERROR: Unable to index line:{docData['content']}")
        raise


def lambda_handler(event, context):
    credentials = boto3.Session().get_credentials()

    # Set up connection to S3 bucket where raw json is stored
    s3 = boto3.client('s3')

    # set up connection to AWS Elasticsearch
    awsauth = AWS4Auth(credentials.access_key,
                       credentials.secret_key,
                       globalVars['awsRegion'],
                       globalVars['service'],
                       session_token=credentials.token
                       )
    logger.info("Received event: " + json.dumps(event, indent=2))

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'])

        # Get document (obj) form S3
        obj = s3.get_object(Bucket=bucket, Key=key)

    except Exception as e:
        logger.error('ERROR: {0}'.format(str(e)))
        logger.error(
            'ERROR: Unable able to GET object:{0} from S3 Bucket:{1}. Verify object exists.'.format(key, bucket))

    # Read object from event
    body = obj['Body'].read().decode("utf-8")
    logger.info('SUCCESS: Retreived object from S3')

    # Create elasticsearch document headers
    docData = {}
    docData['objectKey'] = str(key)
    docData['createdDate'] = str(obj['LastModified'])
    docData['content_type'] = str(obj['ContentType'])
    docData['content_length'] = str(obj['ContentLength'])

    # parsing content before sending to Elasticsearch
    temp = json.loads(body)

    # Load image to S3 for future processing
    image = temp['profile_picture_url']
    ext = re.search(r'\.\w{3,4}(?=\?)', image).group()
    img_key = 'instagram_graph_image_store/' + \
        str(temp['id']) + '/' + "profile" + ext

    # Check for existing profile image
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=img_key,
    )
    if response.get('KeyCount', []) < 1:
        response = requests.get(image).content
        s3.put_object(Bucket=bucket, Key=img_key, Body=response)
        logger.info(f"Added {img_key + '/profile.jpg'} to S3")
    else:
        logger.info(
            f"Found the file {img_key} already in S3")

    # Remove uneeded columns before sending to Elasticsearch
    temp.pop("ig_id", None)
    temp.pop("email_contacts", None)
    temp.pop("phone_call_clicks", None)
    temp.pop("text_message_clicks", None)
    temp.pop("get_directions_clicks", None)
    temp.pop("website_clicks", None)
    temp.pop("profile_views", None)

    # Add new image URL to ES document
    temp['profile_picture'] = 'https://social-system-test.s3.amazonaws.com/' + img_key

    # package up the document data
    docData['content'] = temp

    # Build Elasticsearch URL
    es_Url = globalVars['esHosts'].get(
        'aws') + globalVars['esIndexPrefix'] + globalVars['esIndexDocType'] + '/' + str(temp['id'])

    # send to AWS ES
    logger.info('Calling AWS ES...')
    indexDocElement(es_Url, awsauth, docData)
    logger.info('SUCCESS: Successfully indexed the entire doc into ES')


if __name__ == '__main__':
    lambda_handler(None, None)
