import io
import logging
import pandas
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.schema import Table
from modules.ColumnTypeResolver import ColumnTypeResolver
from modules.data_sources.ChangeTrackingInfo import ChangeTrackingInfo
from sqlalchemy.sql import text

class MsSqlDataSource(object):

    def __init__(self, connection_string, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.connection_string = connection_string
        self.database_engine = create_engine(connection_string)
        self.column_type_resolver = ColumnTypeResolver()

    @staticmethod
    def can_handle_connection_string(connection_string):
        return connection_string.startswith(MsSqlDataSource.connection_string_prefix())

    @staticmethod
    def connection_string_prefix():
        return 'mssql+pyodbc://'

    @staticmethod
    def prefix_column(column_name, full_refresh, primary_key_column_name):
        if column_name == primary_key_column_name and not full_refresh:
            return "chg.{0}".format(column_name)
        else:
            return "t.{0}".format(column_name)

    def build_select_statement(self, table_configuration, columns, batch_configuration, previous_batch_key, full_refresh, change_tracking_info):
        column_array = list(map(lambda cfg: self.prefix_column(cfg['source_name'], full_refresh, table_configuration['primary_key']), columns))
        column_names = ", ".join(column_array)
        column_names = "{0}, {1} as data_pipeline_next_change_minimum_version".format(column_names, change_tracking_info.next_sync_version)
        if full_refresh:
            return "SELECT TOP ({0}) {1} FROM {2}.{3} t WHERE t.{4} > {5} ORDER BY t.{4}".format(batch_configuration['size'],
                                                                                           column_names,
                                                                                           table_configuration[
                                                                                               'schema'],
                                                                                           table_configuration[
                                                                                               'name'],
                                                                                           table_configuration[
                                                                                               'primary_key'],
                                                                                           previous_batch_key)
        else:
            sql_builder = io.StringIO()
            sql_builder.write("SELECT TOP ({0}) {1}, ".format(batch_configuration['size'], column_names))
            sql_builder.write("chg.SYS_CHANGE_VERSION as data_pipeline_change_version, CASE chg.SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END as data_pipeline_is_deleted \n")
            sql_builder.write("FROM CHANGETABLE(CHANGES {0}.{1}, {2}) chg ".format(table_configuration['schema'],
                                                                                   table_configuration['name'],
                                                                                   change_tracking_info.this_sync_version))
            sql_builder.write(" LEFT JOIN {0}.{1} t on chg.{2} = t.{2} ".format( table_configuration['schema'],
                                                                                           table_configuration['name'],
                                                                                           table_configuration['primary_key'],))

            sql_builder.write("WHERE chg.{0} > {1} ORDER BY chg.{0}".format(table_configuration['primary_key'],
                                                                                        previous_batch_key))

            return sql_builder.getvalue()

    # Returns an array of configured_columns containing only columns that this data source supports. Logs invalid ones.
    def assert_data_source_is_valid(self, table_configuration, configured_columns):
        columns_in_database = self.get_table_columns(table_configuration)

        for column in configured_columns:
            self.assert_column_exists(column['source_name'], columns_in_database, "{0}.{1}".format(table_configuration['schema'], table_configuration['name']))

    def assert_column_exists(self, column_name, columns_in_database, table_name):
        if column_name in columns_in_database:
            return True

        message = 'Column {0} does not exist in source {1}'.format(column_name, table_name)
        raise ValueError(message)

    def get_table_columns(self, table_configuration):
        metadata = MetaData()
        self.logger.debug("Reading definition for source table {0}.{1}".format(table_configuration['schema'],
                                                                               table_configuration['name']))
        table = Table(table_configuration['name'], metadata, schema=table_configuration['schema'], autoload=True,
                      autoload_with=self.database_engine)
        return list(map(lambda column: column.name, table.columns))


    def get_next_data_frame(self, table_configuration, columns, batch_configuration, batch_tracker, previous_batch_key, full_refresh, change_tracking_info):
        sql = self.build_select_statement(table_configuration, columns, batch_configuration, previous_batch_key, full_refresh, change_tracking_info,)
        self.logger.debug("Starting read of SQL Statement: {0}".format(sql))
        data_frame = pandas.read_sql_query(sql, self.database_engine)

        self.logger.debug("Completed read")

        batch_tracker.extract_completed_successfully(len(data_frame))

        return data_frame

    def init_change_tracking(self, table_configuration, last_sync_version):

        sql_builder = io.StringIO()
        sql_builder.write(
            "IF NOT EXISTS(SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('{0}.{1}'))\n".format(
                table_configuration['schema'], table_configuration['name']))
        sql_builder.write("BEGIN\n")
        sql_builder.write("ALTER TABLE {0}.{1} ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED=OFF);\n".format(
            table_configuration['schema'], table_configuration['name']))
        sql_builder.write("END\n")

        self.database_engine.execute(text(sql_builder.getvalue()).execution_options(autocommit=True))

        sql_builder = io.StringIO()
        sql_builder.write("DECLARE @last_sync_version bigint = {0}; \n".format(last_sync_version))
        sql_builder.write("DECLARE @this_sync_version bigint = 0; \n")
        sql_builder.write("DECLARE @next_sync_version bigint = CHANGE_TRACKING_CURRENT_VERSION(); \n")
        sql_builder.write("IF @last_sync_version >= CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('{0}.{1}'))\n".format(table_configuration['schema'],table_configuration['name']))
        sql_builder.write(" SET @this_sync_version = @last_sync_version; \n")
        sql_builder.write(" SELECT @next_sync_version as next_sync_version, @this_sync_version as this_sync_version; \n")

        self.logger.debug("Getting ChangeTrackingInformation for {0}.{1}. {2}".format(table_configuration['schema'],
                                                                                      table_configuration['name'],
                                                                                      sql_builder.getvalue()))

        result = self.database_engine.execute(sql_builder.getvalue())
        row = result.fetchone()
        sql_builder.close()

        return ChangeTrackingInfo(row["this_sync_version"], row["next_sync_version"])
