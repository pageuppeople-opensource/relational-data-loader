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
    def prefix_column(column_name, full_refresh, primary_key_column_names):
        if column_name in primary_key_column_names and not full_refresh:
            return f"chg.{column_name}"
        else:
            return f"t.{column_name}"

    def build_select_statement(self, table_configuration, columns, batch_configuration, batch_key_tracker, full_refresh,
                               change_tracking_info):
        column_array = list(
            map(lambda cfg: self.prefix_column(cfg['source_name'], full_refresh, table_configuration['primary_keys']),
                columns))
        column_names = ", ".join(column_array)

        if full_refresh:
            order_by = ", t.".join(table_configuration['primary_keys'])
            return f"SELECT TOP ({batch_configuration['size']}) {column_names} " \
                f"FROM {table_configuration['schema']}.{table_configuration['name']} t " \
                f"WHERE {self.build_where_clause(batch_key_tracker, 't')} " \
                f"ORDER BY {order_by}"
        else:
            order_by = ", chg.".join(table_configuration['primary_keys'])

            sql_builder = io.StringIO()
            sql_builder.write("SELECT TOP ({0}) {1}, ".format(batch_configuration['size'], column_names))
            sql_builder.write(
                "chg.SYS_CHANGE_VERSION as data_pipeline_change_version, CASE chg.SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END as data_pipeline_is_deleted \n")
            sql_builder.write("FROM CHANGETABLE(CHANGES {0}.{1}, {2}) chg ".format(table_configuration['schema'],
                                                                                   table_configuration['name'],
                                                                                   change_tracking_info.this_sync_version))
            sql_builder.write(" LEFT JOIN {0}.{1} t on {2} ".format(table_configuration['schema'],
                                                                                table_configuration['name'],
                                                                    self.build_change_table_on_clause(batch_key_tracker)))

            sql_builder.write("WHERE {0}".format(self.build_where_clause(batch_key_tracker, "t")))
            sql_builder.write("ORDER BY {0}".format(order_by))

            return sql_builder.getvalue()

    # Returns an array of configured_columns containing only columns that this data source supports. Logs invalid ones.
    def assert_data_source_is_valid(self, table_configuration, configured_columns):
        columns_in_database = self.get_table_columns(table_configuration)

        for column in configured_columns:
            self.assert_column_exists(column['source_name'],
                                      columns_in_database,
                                      f"{table_configuration['schema']}.{table_configuration['name']}")

    def assert_column_exists(self, column_name, columns_in_database, table_name):
        if column_name in columns_in_database:
            return True

        message = f'Column {column_name} does not exist in source table {table_name}'
        raise ValueError(message)

    def get_table_columns(self, table_configuration):
        metadata = MetaData()
        self.logger.debug(f"Reading definition for source table "
                          f"{table_configuration['schema']}.{table_configuration['name']}")
        table = Table(table_configuration['name'], metadata, schema=table_configuration['schema'], autoload=True,
                      autoload_with=self.database_engine)
        return list(map(lambda column: column.name, table.columns))

    def get_next_data_frame(self, table_configuration, columns, batch_configuration, batch_tracker, batch_key_tracker,
                            full_refresh, change_tracking_info):
        sql = self.build_select_statement(table_configuration, columns, batch_configuration, batch_key_tracker,
                                          full_refresh, change_tracking_info, )
        self.logger.debug(f"Starting read of SQL Statement: \n{sql}")
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
        sql_builder.write("IF @last_sync_version >= CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('{0}.{1}'))\n".format(
            table_configuration['schema'], table_configuration['name']))
        sql_builder.write(" SET @this_sync_version = @last_sync_version; \n")
        sql_builder.write(
            " SELECT @next_sync_version as next_sync_version, @this_sync_version as this_sync_version; \n")

        self.logger.debug("Getting ChangeTrackingInformation for {0}.{1}. {2}".format(table_configuration['schema'],
                                                                                      table_configuration['name'],
                                                                                      sql_builder.getvalue()))

        result = self.database_engine.execute(sql_builder.getvalue())
        row = result.fetchone()
        sql_builder.close()

        force_full_load = bool(row["this_sync_version"] == 0 or row["next_sync_version"] == 0)
        return ChangeTrackingInfo(row["this_sync_version"], row["next_sync_version"], force_full_load)

    @staticmethod
    def build_where_clause(batch_key_tracker, table_alias):
        has_value = False

        try:
            sql_builder = io.StringIO()
            for primary_key in batch_key_tracker.bookmarks:
                if has_value:
                    sql_builder.write(" AND ")

                sql_builder.write(
                    f" {table_alias}.{primary_key} > {batch_key_tracker.bookmarks[primary_key]}")
                has_value = True

            return sql_builder.getvalue()
        finally:
            sql_builder.close()

    @staticmethod
    def build_change_table_on_clause(batch_key_tracker):
        has_value = False

        try:
            sql_builder = io.StringIO()
            for primary_key in batch_key_tracker.bookmarks:
                if has_value:
                    sql_builder.write(" AND ")

                sql_builder.write(f" chg.{primary_key} = t.{primary_key}")
                has_value = True

            return sql_builder.getvalue()
        finally:
            sql_builder.close()
