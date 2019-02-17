import io
import logging
import pandas
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.schema import Table
from modules.ColumnTypeResolver import ColumnTypeResolver
from modules.data_sources.ChangeTrackingInfo import ChangeTrackingInfo
from sqlalchemy.sql import text
from modules.Shared import Constants


class MsSqlDataSource(object):
    SOURCE_TABLE_ALIAS = 'src'
    CHANGE_TABLE_ALIAS = 'chg'

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
        if not isinstance(primary_key_column_names, (list, tuple)):
            raise TypeError(f"Argument 'primary_key_column_names' must be a list or tuple")
        if column_name in primary_key_column_names and not full_refresh:
            return f"{MsSqlDataSource.CHANGE_TABLE_ALIAS}.{column_name}"
        else:
            return f"{MsSqlDataSource.SOURCE_TABLE_ALIAS}.{column_name}"

    def build_select_statement(self, table_config, columns, batch_config, batch_key_tracker, full_refresh,
                               change_tracking_info):
        column_array = list(
            map(lambda cfg: self.prefix_column(cfg['source_name'], full_refresh, table_config['primary_keys']),
                columns))
        column_names = ", ".join(column_array)

        if full_refresh:
            select_sql = f"SELECT TOP ({batch_config['size']}) {column_names}"
            from_sql = f"FROM {table_config['schema']}.{table_config['name']} AS {MsSqlDataSource.SOURCE_TABLE_ALIAS}"
            where_sql = f"WHERE {self.build_where_clause(batch_key_tracker, MsSqlDataSource.SOURCE_TABLE_ALIAS)}"
            order_by_sql = "ORDER BY " + f", {MsSqlDataSource.SOURCE_TABLE_ALIAS}.".join(table_config['primary_keys'])
        else:
            select_sql = f"SELECT TOP ({batch_config['size']}) {column_names}, " \
                f"{MsSqlDataSource.CHANGE_TABLE_ALIAS}.SYS_CHANGE_VERSION AS {Constants.AuditColumnNames.CHANGE_VERSION}, " \
                f"CASE {MsSqlDataSource.CHANGE_TABLE_ALIAS}.SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 " \
                f"END AS {Constants.AuditColumnNames.IS_DELETED}"
            from_sql = f"FROM CHANGETABLE(CHANGES" \
                f" {table_config['schema']}.{table_config['name']}," \
                f" {change_tracking_info.this_sync_version})" \
                f" AS {MsSqlDataSource.CHANGE_TABLE_ALIAS}" \
                f" LEFT JOIN {table_config['schema']}.{table_config['name']} AS {MsSqlDataSource.SOURCE_TABLE_ALIAS}" \
                f" ON {self.build_change_table_on_clause(batch_key_tracker)}"
            where_sql = f"WHERE {self.build_where_clause(batch_key_tracker, MsSqlDataSource.CHANGE_TABLE_ALIAS)}"
            order_by_sql = "ORDER BY " + f", {MsSqlDataSource.CHANGE_TABLE_ALIAS}.".join(table_config['primary_keys'])

        return f"{select_sql} \n {from_sql} \n {where_sql} \n {order_by_sql};"

    # Returns an array of configured_columns containing only columns that this data source supports. Logs invalid ones.
    def assert_data_source_is_valid(self, table_config, configured_columns):
        columns_in_database = self.get_table_columns(table_config)

        for column in configured_columns:
            self.assert_column_exists(column['source_name'],
                                      columns_in_database,
                                      f"{table_config['schema']}.{table_config['name']}")

    def assert_column_exists(self, column_name, columns_in_database, table_name):
        if column_name in columns_in_database:
            return True

        message = f'Column {column_name} does not exist in source table {table_name}'
        raise ValueError(message)

    def get_table_columns(self, table_config):
        metadata = MetaData()
        self.logger.debug(f"Reading definition for source table "
                          f"{table_config['schema']}.{table_config['name']}")
        table = Table(table_config['name'], metadata, schema=table_config['schema'], autoload=True,
                      autoload_with=self.database_engine)
        return list(map(lambda column: column.name, table.columns))

    def get_next_data_frame(self, table_config, columns, batch_config, batch_tracker, batch_key_tracker,
                            full_refresh, change_tracking_info):
        sql = self.build_select_statement(table_config, columns, batch_config, batch_key_tracker,
                                          full_refresh, change_tracking_info)

        self.logger.debug(f"Starting read of SQL Statement: \n{sql}")
        data_frame = pandas.read_sql_query(sql, self.database_engine)
        self.logger.debug("Completed read")

        batch_tracker.extract_completed_successfully(len(data_frame))

        return data_frame

    def init_change_tracking(self, table_config, last_sync_version):

        init_change_tracking_sql = "IF NOT EXISTS(SELECT 1 FROM sys.change_tracking_tables " \
            f"WHERE object_id = OBJECT_ID('{table_config['schema']}.{table_config['name']}'))\n" \
                                   "BEGIN\n" \
            f"ALTER TABLE {table_config['schema']}.{table_config['name']} " \
            f"ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED=OFF);\n" \
                                   "END\n"

        self.logger.debug(f"Initializing ChangeTracking for "
                          f"{table_config['schema']}.{table_config['name']}:\n"
                          f"{init_change_tracking_sql}")
        self.database_engine.execute(text(init_change_tracking_sql).execution_options(autocommit=True))

        # in the following we check if we have lost tracking of the table
        get_change_tracking_info_sql = io.StringIO()

        # last-sync-version i.e. the tracking number of the last time we ran rdl
        # it's value was sourced from CHANGE_TRACKING_CURRENT_VERSION()
        get_change_tracking_info_sql.write(f"DECLARE @last_sync_version bigint = {last_sync_version}; \n")

        # If we need to do a full load, this will be changed to @next_sync_version after a full load
        # If we don't need to do a full load, this is changed in this query to @last_sync_version and later updated
        # to @next_sync_version after an incremental load
        # also, by default, assume we have lost tracking of the table
        get_change_tracking_info_sql.write("DECLARE @this_sync_version bigint = 0; \n")

        # by default, assume we have lost tracking of the table
        get_change_tracking_info_sql.write("DECLARE @force_full_load bit = 1; \n")

        # CHANGE_TRACKING_CURRENT_VERSION gets the tracking number of the database
        # each time mssql tracks a change in the db (in any table), the number is incremented by one
        # next_sync_version - last_sync_version = number of mssql tracked db changes since rdl was run
        get_change_tracking_info_sql.write("DECLARE @next_sync_version bigint = CHANGE_TRACKING_CURRENT_VERSION(); \n")

        # CHANGE_TRACKING_MIN_VALID_VERSION is the minimum tracking number that we can use to update our db from
        # e.g. if a bunch of changes happen to the db, the tracking number will increase, at some point
        # our record of the db may become so far out of sync that we are unable to salvage our db
        # in that case @last_sync_version < CHANGE_TRACKING_MIN_VALID_VERSION and we need to do a full load
        # therefore if @last_sync_version >= CHANGE_TRACKING_MIN_VALID_VERSION, we do not need to do a full load
        get_change_tracking_info_sql.write(f"DECLARE @min_valid_version bigint = CHANGE_TRACKING_MIN_VALID_VERSION("
                                           f"OBJECT_ID('{table_config['schema']}.{table_config['name']}')); \n")
        get_change_tracking_info_sql.write(f"IF @last_sync_version >= @min_valid_version\n")
        get_change_tracking_info_sql.write("BEGIN\n")
        get_change_tracking_info_sql.write("     SET @force_full_load = 0; \n")
        get_change_tracking_info_sql.write("     SET @this_sync_version = @last_sync_version; \n")
        get_change_tracking_info_sql.write("END\n")
        get_change_tracking_info_sql.write("SELECT @next_sync_version as next_sync_version,"
                                           "@force_full_load as force_full_load,"
                                           "@this_sync_version as this_sync_version; \n")

        self.logger.debug("Getting ChangeTracking info for "
                          f"{table_config['schema']}.{table_config['name']}.\n"
                          f"{get_change_tracking_info_sql.getvalue()}")

        result = self.database_engine.execute(get_change_tracking_info_sql.getvalue())
        row = result.fetchone()
        get_change_tracking_info_sql.close()

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

                sql_builder.write(f" {MsSqlDataSource.CHANGE_TABLE_ALIAS}.{primary_key} ="
                                  f" {MsSqlDataSource.SOURCE_TABLE_ALIAS}.{primary_key}")
                has_value = True

            return sql_builder.getvalue()
        finally:
            sql_builder.close()
