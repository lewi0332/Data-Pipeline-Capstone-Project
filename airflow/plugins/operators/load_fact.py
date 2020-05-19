from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 query="",
                 truncate="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query
        self.truncate = truncate

    def execute(self, context):
        """
        Explain this 
        """
        self.log.info('LoadFactOperator Starting')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        redshift.run(self.query)
        self.log.info('LoadFactOperator complete')
