from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 file_type="json",
                 redshift_role="",
                 delimiter=",",
                 ignore_headers=1,
                 json_path='auto',
                 * args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_type = file_type
        self.redshift_role = redshift_role
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.json_path = json_path

    def execute(self, context):
        """ 
        Write Description here
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        self.log.info(f"Rendered key: {rendered_key}")
        self.log.info(f"Role for Redshift Parquet: {self.redshift_role}")
        s3_path = "s3://" + self.s3_bucket + rendered_key
        self.log.info(f"Retrieving data from {s3_path}")
        if self.file_type.lower() == 'json':
            redshift.run(f"\
                          COPY {self.table} \
                          FROM '{s3_path}'\
                          ACCESS_KEY_ID '{credentials.access_key}'\
                          SECRET_ACCESS_KEY '{credentials.secret_key}'\
                          REGION '{self.region}'\
                          TIMEFORMAT as 'epochmillisecs'\
                          TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL\
                          FORMAT as JSON '{self.json_path}';")
        elif self.file_type.lower() == 'csv':
            redshift.run(f"\
                         COPY {self.table} \
                         FROM '{s3_path}'\
                         ACCESS_KEY_ID '{credentials.access_key}'\
                         SECRET_ACCESS_KEY '{credentials.secret_key}'\
                         REGION '{self.region}'\
                         TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL\
                         FORMAT AS CSV \
                         IGNOREHEADER {self.ignore_headers}\
                         DELIMITER '{self.delimiter}';")
        elif self.file_type.lower() == 'parquet':
            redshift.run(f"\
                         COPY {self.table} \
                         FROM '{s3_path}'\
                         IAM_ROLE '{self.redshift_role}'\
                         FORMAT AS PARQUET;")
        else:
            self.log.info("Unkown file format in S3")
            raise
        self.log.info('StageToRedshiftOperator implemented')
