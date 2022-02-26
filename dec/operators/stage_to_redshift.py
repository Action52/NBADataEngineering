from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

from dec.sql.staging_queries import copy_raw_table
from dec.sql.create_queries import create_raw_all_star_table, create_raw_players_1_table, create_raw_players_2_table, \
    create_raw_shots_table, create_raw_teams_table


class RedshiftStagingOperator(BaseOperator):
    ui_color = '#38f5d9'

    @apply_defaults
    def __init__(self, db_conn_id="", aws_credentials_id="", table="", s3_bucket="", s3_key="", create_query=None,
                 copy_query=None, clean=False, skip=False, arn="", *args, **kwargs):
        """
        Initiates the operator.
        :param db_conn_id: Connection to the Redshift database saved in Airflow.
        :param aws_credentials_id: Connection to the aws credentials saved in Airflow.
        :param table: Table name.
        :param s3_bucket: Bucket to download the raw data.
        :param s3_key: s3 Key from which download the raw data.
        :param delimiter: Delimiter to use.
        :param ignore_headers: If passed, states if we want to ignore the headers.
        :param clean: Bool, determines if we shall clean the data from a previously existing table.
        :param staging_type: songs or logs
        :param json_conf: If we are staging the logs data, we have to pass the json map for this data. Full s3 path.
        :param skip: Skips the step if passed.
        :param args:
        :param kwargs:
        """
        super(RedshiftStagingOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.clean = clean
        self.skip = skip
        self.create_query = create_query
        self.copy_query = copy_query
        self.arn = arn

    def execute(self, context):
        """
        Ingests the data from s3 into Redshift.
        :param context:
        :return:
        """
        if not self.skip:
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            db = PostgresHook(self.db_conn_id)
            self.log.info("Creating table if not exists.")
            db.run(self.create_query(self.table))
            if self.clean:
                self.log.info("Clearing table content.")
                db.run(self.clean_table())
            self.log.info("Streaming data from s3 to db.")
            rendered_key = self.s3_key.format(**context)
            s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
            self.log.info(f"Copying data from {s3_path}.")
            db.run(self.copy_query(self.table, s3_path, self.arn))
            self.log.info(f"Data copied successfully into {self.table}")
        else:
            self.log.info(f"Skipping step after user selection.")

    def clean_table(self):
        return f"DELETE FROM {self.table};"
