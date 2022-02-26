from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable, Connection
from airflow import settings

from dec.exceptions import ConfigValueNotFoundException


class CheckConfigurationOperator(BaseOperator):
    @apply_defaults
    def __init__(self, config, *args, **kwargs):
        super(CheckConfigurationOperator, self).__init__(*args, **kwargs)
        self.config = config

    def execute(self, context):
        self.check_config()
        self.setup_connections()

    def check_config(self):
        # Try reading the configuration file
        try:
            # AWS
            aws_access_key = self.config.get("aws_access_key")
            aws_secret_access_key = self.config.get("aws_secret_access_key")
            # Redshift
            redshift_db = self.config.get("redshift_db")
            redshift_user = self.config.get("redshift_user")
            redshift_pswd = self.config.get("redshift_pswd")
            redshift_host = self.config.get("redshift_host")
            redshift_port = self.config.get("redshift_port")
            # S3
            s3_bucket = self.config.get("s3_bucket")
            s3_shots_key = self.config.get("s3_shots_key")
            s3_players_1_key = self.config.get("s3_players_1_key")
            s3_players_2_key = self.config.get("s3_players_2_key")
            s3_all_star_key = self.config.get("s3_players_1_key")
            s3_historical_key = self.config.get("s3_historical_key")
            # Stages
            skip_stage_3 = self.config.get("skip_stage_3")
            skip_stage_4 = self.config.get("skip_stage_4")
            skip_stage_5 = self.config.get("skip_stage_5")
            skip_stage_6 = self.config.get("skip_stage_6")
            skip_stage_7 = self.config.get("skip_stage_7")
            skip_stage_8 = self.config.get("skip_stage_8")
            skip_stage_9 = self.config.get("skip_stage_9")
        except KeyError:
            raise ConfigValueNotFoundException("The configuration is missing a key-value pair.")

    def setup_connections(self):
        session = settings.Session()
        try:
            existing = (
                session.query(Connection)
                .filter(Connection.conn_id == self.config.get("redshift_conn_id"))
                .all()
            )
            if not existing:
                self.log.info("Couldn't find any existing redshift connection, creating a new one.")
                redshift = Connection(conn_id=self.config.get("redshift_conn_id"), conn_type="Postgres",
                                      host=self.config.get("redshift_host"),
                                      login=self.config.get("redshift_user"),
                                      password=self.config.get("redshift_pswd"),
                                      port=self.config.get("redshift_port"),
                                      schema=self.config.get("redshift_db"))
                session.add(redshift)
                session.commit()
            else:
                self.log.info("Using existing redshift connection.")
        except Exception as e:
            print(e)
        try:
            existing = (
                session.query(Connection)
                .filter(Connection.conn_id == self.config.get("aws_credentials_id"))
                .all()
            )
            if not existing:
                self.log.info("Couldn't find any existing aws connection, creating a new one.")
                aws = Connection(conn_id=self.config.get("aws_credentials_id"), conn_type="Amazon Web Services",
                                 login=self.config.get("aws_access_key"),
                                 password=self.config.get("aws_secret_access_key"))
                session.add(aws)
                session.commit()
            else:
                self.log.info("Using existing aws connection.")
        except Exception as e:
            print(e)

