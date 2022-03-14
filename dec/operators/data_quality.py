from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

import boto3, os


class DataQualityCheckAfterStreamingOperator(BaseOperator):
    @apply_defaults
    def __init__(self, aws_credentials_id="", db_conn_id="", s3_bucket="", csvs=[], tables=[], skip=False,*args, **kwargs):
        super(DataQualityCheckAfterStreamingOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.db_conn_id = db_conn_id
        self.s3_bucket = s3_bucket
        self.csvs = csvs
        self.tables = tables
        self.skip = skip

    def execute(self, context):
        if not self.skip:
            db = PostgresHook(self.db_conn_id)
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            s3 = boto3.client('s3',
                              aws_access_key_id=credentials.access_key,
                              aws_secret_access_key=credentials.secret_key)
            for csv, table in zip(self.csvs, self.tables):
                sql_statement = """
                    SELECT COUNT(*) FROM s3object S 
                """
                query = s3.select_object_content(
                    Bucket=self.s3_bucket,
                    Key=csv,
                    ExpressionType="SQL",
                    Expression=sql_statement,
                    InputSerialization={'CSV': {'AllowQuotedRecordDelimiter': True}},
                    OutputSerialization={'CSV': {
                        'RecordDelimiter': os.linesep,
                        'FieldDelimiter': ","
                    }},
                )
                db_count = db.get_records(f"SELECT COUNT(*) FROM {table}")
                for event in query['Payload']:
                    self.log.info(event)
                    if 'Records' in event:
                        response = event['Records']['Payload'].decode('utf-8')
                        for i, rec in enumerate(response.split(os.linesep)):
                            if rec:
                                row = rec.split(",")
                                if row:
                                    self.log.info(f"{table} row length: {db_count[0][0]}. {csv} row length: {row[0]}")
                                    assert db_count[0][0] > 0
        else:
            self.log.info(f"Skipping step after user selection.")


class DataQualityCheckDBOperator(BaseOperator):
    @apply_defaults
    def __init__(self, db_conn_id="", tables=[], skip=False, *args, **kwargs):
        super(DataQualityCheckDBOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.tables = tables
        self.skip = skip

    def execute(self, context):
        if not self.skip:
            db = PostgresHook(self.db_conn_id)
            for tables in self.tables:
                self.log.info(f"{tables[0]}, {tables[1]}")
                db_count = db.get_records(f"SELECT COUNT(*) FROM {tables[0]}")
                if len(tables) == 3:
                    db_count_raw_1 = db.get_records(f"SELECT COUNT(*) FROM {tables[1]}")
                    db_count_raw_2 = db.get_records(f"SELECT COUNT(*) FROM {tables[2]}")
                    self.log.info(f"{tables}, {db_count_raw_1}, {db_count_raw_2}, {db_count}")
                    assert (db_count_raw_1[0][0] + db_count_raw_2[0][0]) >= db_count[0][0]
                else:
                    db_count_raw = db.get_records(f"SELECT COUNT(*) FROM {tables[1]}")
                    assert db_count_raw[0][0] >= db_count[0][0]
        else:
            self.log.info(f"Skipping step after user selection.")
