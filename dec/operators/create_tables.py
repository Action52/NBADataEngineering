from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateDimensionOperator(BaseOperator):
    ui_color = '#38f5d9'

    @apply_defaults
    def __init__(self, db_conn_id="", dimension="", dimension_query=None, clean=False, skip=False, *args, **kwargs):
        """
        Initiates the operator.
        :param db_conn_id: Connection to the Redshift database saved in Airflow.
        :param table: Table name.
        :param delimiter: Delimiter to use.
        :param ignore_headers: If passed, states if we want to ignore the headers.
        :param clean: Bool, determines if we shall clean the data from a previously existing table.
        :param staging_type: songs or logs
        :param skip: Skips the step if passed.
        :param args:
        :param kwargs:
        """
        super(CreateDimensionOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.dimension = dimension
        self.clean = clean
        self.skip = skip
        self.dimension_query = dimension_query

    def execute(self, context):
        """
        Ingests the data from s3 into Redshift.
        :param context:
        :return:
        """
        if not self.skip:
            db = PostgresHook(self.db_conn_id)
            self.log.info("Creating table if not exists.")
            db.run(self.dimension_query(self.dimension))
        else:
            self.log.info(f"Skipping step after user selection.")

    def clean_table(self):
        return f"DELETE FROM {self.dimension};"


class CreateFactOperator(BaseOperator):
    ui_color = '#38f5d9'

    @apply_defaults
    def __init__(self, db_conn_id="", fact="", historical_table="", all_star_table="", player_table="",
                 fact_query=None, clean=False, skip=False, *args, **kwargs):
        """
        Initiates the operator.
        :param db_conn_id: Connection to the Redshift database saved in Airflow.
        :param table: Table name.
        :param delimiter: Delimiter to use.
        :param ignore_headers: If passed, states if we want to ignore the headers.
        :param clean: Bool, determines if we shall clean the data from a previously existing table.
        :param staging_type: songs or logs
        :param skip: Skips the step if passed.
        :param args:
        :param kwargs:
        """
        super(CreateFactOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.fact = fact
        self.historical_table = historical_table
        self.player_table = player_table
        self.all_star_table = all_star_table
        self.clean = clean
        self.skip = skip
        self.fact_query = fact_query

    def execute(self, context):
        """
        Ingests the data from s3 into Redshift.
        :param context:
        :return:
        """
        if not self.skip:
            db = PostgresHook(self.db_conn_id)
            self.log.info("Creating table if not exists.")
            db.run(self.fact_query(self.fact, self.player_table, self.historical_table, self.all_star_table))
        else:
            self.log.info(f"Skipping step after user selection.")

    def clean_table(self):
        return f"DELETE FROM {self.fact};"
