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
globalVars['tagName'] = "serverless-s3-to-es-log-ingester"
globalVars['service'] = "es"
globalVars['esIndexPrefix'] = "/instagram_graph_posts/"
globalVars['esIndexDocType'] = "_doc"
globalVars['esHosts'] = {
    'prod': 'something more secure here',
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
        # headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
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


def store_images(temp, bucket):
    """
    Stores images from media url to S3 bucket. 
    Checks for existing image in S3, loads it if absent
    Then checks if "children" Media urls are present and saves as such: 
    17841401753941377 / 10010784827803388 / original.jpg, 17870925466585765.jpg, 18015701020271224.jpg

    PARAMS:
    temp - dictionary object loaded from from S3 with post data
    bucket - bucket to store loaded images

    RETURNS: 
    New url of media object stored on S3
    """

    s3 = boto3.client('s3')

    if 'media_url' in temp.keys():
        image = temp['media_url']
    elif 'thumbnail_url' in temp.keys():
        image = temp['thumbnail_url']
    else:
        return 'image load fail'

    img_key = 'instagram_graph_image_store/' + \
        str(temp['owner_id']) + '/' + str(temp['id'])

    # Get file extension (.jpg or .mp4)
    ext = re.search(r'\.\w{3,4}(?=\?)', image).group()

    # look for photos already in bucket
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=img_key,
    )
    # if key count 0, there is no photo already stored in s3
    if response.get('KeyCount', []) < 1:
        response = requests.get(image).content
        s3.put_object(Bucket=bucket, Key=img_key +
                      '/original' + ext, Body=response)
        logger.info(f"Added {img_key + '/original.jpg'} to S3")

        # if this is a carousel post, get all child images
        if 'children' in temp.keys():
            for i in range(len(temp['children']['data'])):
                img_id = temp['children']['data'][i]['id']
                image = temp['children']['data'][i]['media_url']
                ext = re.search(r'\.\w{3,4}(?=\?)', image).group()
                tmp_key = img_key + '/' + img_id + ext
                response = requests.get(image).content
                s3.put_object(Bucket=bucket, Key=tmp_key, Body=response)
        # if this is a video get the thumbnail
        elif 'thumbnail_url' in temp.keys():
            img_id = 'thumbnail'
            image = temp['thumbnail_url']
            ext = re.search(r'\.\w{3,4}(?=\?)', image).group()
            tmp_key = img_key + '/' + img_id + ext
            response = requests.get(image).content
            s3.put_object(Bucket=bucket, Key=tmp_key, Body=response)
        else:
            logger.info('No carousel images found')
    else:
        logger.info(
            f"Found the file {img_key + '/original.jpg'} already in S3")
    return 'https://social-system-test.s3.amazonaws.com/' + img_key + '/original' + ext


def lambda_handler(event, context):
    credentials = boto3.Session().get_credentials()

    # Set up connection to S3 bucket with raw json
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

        # Get document (obj) from S3
        obj = s3.get_object(Bucket=bucket, Key=key)

    except Exception as e:
        logger.error('ERROR: {0}'.format(str(e)))
        logger.error(
            'ERROR: Unable able to GET object:{0} from S3 Bucket:{1}. Verify object exists.'.format(key, bucket))

    # Create read object
    body = obj['Body'].read().decode("utf-8")
    logger.info('SUCCESS: Retreived object from S3')

    # Create document headers
    docData = {}
    docData['objectKey'] = str(key)
    docData['createdDate'] = str(obj['LastModified'])
    docData['content_type'] = str(obj['ContentType'])
    docData['content_length'] = str(obj['ContentLength'])

    # Parsing content before sending to Elasticsearch
    temp = json.loads(body)

    # items not needed in ES.
    temp.pop("ig_id", None)
    temp.pop("username", None)
    temp.pop("is_comment_enabled", None)

    # flatten Owner ID
    temp['owner_id'] = temp['owner']['id']

    # set post id to string
    temp['id'] = str(temp['id'])

    # look for followers, if none set to -1
    try:
        temp['followers'] = temp['owner']['followers_count']
    except Exception as e:
        logger.info(f'Failed to set followers. {e}')
        temp['followers'] = -1
    temp.pop('owner')

    # load images to S3
    temp['fohr_media'] = store_images(temp, bucket)

    # package up the document data
    docData['content'] = temp

    # Build Elasticsearch URL
    es_Url = globalVars['esHosts'].get(
        'aws') + globalVars['esIndexPrefix'] + globalVars['esIndexDocType'] + '/' + temp['id']

    # send to AWS ES
    logger.info('Calling AWS ES...')
    indexDocElement(es_Url, awsauth, docData)
    logger.info('SUCCESS: Successfully indexed the entire doc into ES')


if __name__ == '__main__':
    lambda_handler(None, None)
