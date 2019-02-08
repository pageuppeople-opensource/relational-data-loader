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
            sql_builder.write(f"SELECT TOP ({batch_configuration['size']}) {column_names}, ")
            sql_builder.write("chg.SYS_CHANGE_VERSION as data_pipeline_change_version, "
                              "CASE chg.SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 END as data_pipeline_is_deleted \n")
            sql_builder.write(f" FROM CHANGETABLE(CHANGES"
                              f" {table_configuration['schema']}.{table_configuration['name']},"
                              f" {change_tracking_info.this_sync_version})"
                              f" AS chg")
            sql_builder.write(f" LEFT JOIN {table_configuration['schema']}.{table_configuration['name']} t"
                              f" on {self.build_change_table_on_clause(batch_key_tracker)}")
            sql_builder.write(f" WHERE {self.build_where_clause(batch_key_tracker, 'chg')}")
            sql_builder.write(f" ORDER BY {order_by}")

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
        sql_builder.write("IF NOT EXISTS(SELECT 1 FROM sys.change_tracking_tables WHERE "
                          f"object_id = OBJECT_ID('{table_configuration['schema']}.{table_configuration['name']}'))\n")
        sql_builder.write("BEGIN\n")
        sql_builder.write(f"ALTER TABLE {table_configuration['schema']}.{table_configuration['name']} "
                          f"ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED=OFF);\n")
        sql_builder.write("END\n")

        self.logger.debug(f"Enabling ChangeTracking for {table_configuration['schema']}.{table_configuration['name']}\n"
                          f"{sql_builder.getvalue()}")

        self.database_engine.execute(text(sql_builder.getvalue()).execution_options(autocommit=True))

        # in the following we check if we have lost tracking of the table
        sql_builder = io.StringIO()

        # last-sync-version i.e. the tracking number of the last time we ran rdl
        # it's value was sourced from CHANGE_TRACKING_CURRENT_VERSION()
        sql_builder.write(f"DECLARE @last_sync_version bigint = {last_sync_version}; \n")

        # If we need to do a full load, this will be changed to @next_sync_version after a full load
        # If we don't need to do a full load, this is changed in this query to @last_sync_version and later updated
        # to @next_sync_version after an incremental load
        # also, by default, assume we have lost tracking of the table
        sql_builder.write("DECLARE @this_sync_version bigint = 0; \n")

        # by default, assume we have lost tracking of the table
        sql_builder.write("DECLARE @force_full_load bit = 1; \n")

        # CHANGE_TRACKING_CURRENT_VERSION gets the tracking number of the database
        # each time mssql tracks a change in the db (in any table), the number is incremented by one
        # next_sync_version - last_sync_version = number of mssql tracked db changes since rdl was run
        sql_builder.write("DECLARE @next_sync_version bigint = CHANGE_TRACKING_CURRENT_VERSION(); \n")

        # CHANGE_TRACKING_MIN_VALID_VERSION is the minimum tracking number that we can use to update our db from
        # e.g. if a bunch of changes happen to the db, the tracking number will increase, at some point
        # our record of the db may become so far out of sync that we are unable to salvage our db
        # in that case @last_sync_version < CHANGE_TRACKING_MIN_VALID_VERSION
        # therefore if @last_sync_version >= CHANGE_TRACKING_MIN_VALID_VERSION, we do not need to do a full load
        sql_builder.write(f"IF @last_sync_version >= CHANGE_TRACKING_MIN_VALID_VERSION("
                          f"OBJECT_ID('{table_configuration['schema']}.{table_configuration['name']}'))\n")
        sql_builder.write("BEGIN\n")
        sql_builder.write("     SET @force_full_load = 0; \n")
        sql_builder.write("     SET @this_sync_version = @last_sync_version; \n")
        sql_builder.write("END\n")
        sql_builder.write("SELECT @next_sync_version as next_sync_version,"
                          "@force_full_load as force_full_load,"
                          "@this_sync_version as this_sync_version; \n")

        self.logger.debug("Getting ChangeTracking info for "
                          f"{table_configuration['schema']}.{table_configuration['name']}.\n"
                          f"{sql_builder.getvalue()}")

        result = self.database_engine.execute(sql_builder.getvalue())
        row = result.fetchone()
        sql_builder.close()

        return ChangeTrackingInfo(row["this_sync_version"], row["next_sync_version"], row["force_full_load"])

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
