from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
from airflow.contrib.hooks.aws_hook import AwsHook
import boto3
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests
import pandas as pd
from io import StringIO


class GetElasticsearchData(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 es_index="",
                 es_host="",
                 es_port=443,
                 days=90,
                 region="",
                 service_type="",
                 * args, **kwargs):

        super(GetElasticsearchData, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.es_index = es_index
        self.es_host = es_host
        self.es_port = es_port
        self.days = days
        self.region = region
        self.service_type = service_type

    def execute(self, context):
        """ 
        Get ES aggregate data from last 'x' days of posts
        """
        # redshift = PostgresHook(postgres_conn_id=self.conn_id)

        # go get engagement numbers from Elasticsearch
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        awsauth = AWS4Auth(credentials.access_key,
                           credentials.secret_key,
                           'us-east-1',
                           'es',
                           session_token=credentials.token
                           )

        es_client = Elasticsearch(
            hosts=[{'host': self.es_host,
                    'port': self.es_port}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            http_compress=False)

        post_query = {
            "query": {
                "range": {
                    "content.timestamp": {
                        "gte": f"now-{self.days}d"
                    }
                }
            },
            "aggs": {
                "id": {
                    "terms": {
                        "field": "content.owner_id",
                        "size": 10000
                    },
                    "aggregations": {
                        "eng_avg": {
                            "avg": {
                                "field": "content.engagement"
                            }
                        },
                        "fol_avg": {
                            "avg": {
                                "field": "content.followers"
                            }
                        }
                    }
                }
            },
            "size": 0
        }

        es_results = es_client.search(index=self.es_index, body=post_query)

        # flatten ES Results
        for i in es_results['aggregations']['id']['buckets']:
            i['fol_avg'] = i['fol_avg']['value']
            i['eng_avg'] = i['eng_avg']['value']

        df_posts = pd.DataFrame(es_results['aggregations']['id']['buckets'])
        df_posts.columns = ['id', 'doc_count', 'fol_avg', 'eng_avg']

        csv_buffer = StringIO()
        df_posts.to_csv(csv_buffer, index=False)

        self.log.info(
            f"SUCCESS: Successfully got this response from ES in  {es_results['took']} seconds")
        self.log.info(
            "Printing key: temp/post_agg_{run_id}.csv".format(**context))
        # Save Engagement aggregates to S3
        s3 = boto3.client('s3',
                          aws_access_key_id=credentials.access_key,
                          aws_secret_access_key=credentials.secret_key)

        response = s3.put_object(Bucket='social-system-test',
                                 Key='temp/post_agg_{run_id}.csv'.format(
                                     **context),
                                 Body=csv_buffer.getvalue()
                                 )

        self.log.info(
            f"SUCCESS: Successfully put ES data to S3: response {response}")
