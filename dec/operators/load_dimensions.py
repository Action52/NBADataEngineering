from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#38f5d9'

    @apply_defaults
    def __init__(self, db_conn_id="", table="", raw_table1="", raw_table2="",
                 insert_query=None, clean=False, skip=False, *args, **kwargs):
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
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.table = table
        self.raw_table1 = raw_table1
        self.raw_table2 = raw_table2
        self.clean = clean
        self.skip = skip
        self.insert_query = insert_query

    def execute(self, context):
        """
        Ingests the data from s3 into Redshift.
        :param context:
        :return:
        """
        if not self.skip:
            db = PostgresHook(self.db_conn_id)
            if self.clean:
                self.log.info("Clearing table content.")
                db.run(self.clean_table())
            db.run(self.insert_query(self.raw_table1, self.raw_table2, self.table))
            self.log.info(f"Data loaded successfully into {self.table}")
        else:
            self.log.info(f"Skipping step after user selection.")

    def clean_table(self):
        return f"DELETE FROM {self.table};"
