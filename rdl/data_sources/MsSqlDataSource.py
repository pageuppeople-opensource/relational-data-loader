import io
import logging
import pandas
import pyodbc
import re

import sqlalchemy.exc
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.schema import Table
from sqlalchemy.sql import text

from rdl.ColumnTypeResolver import ColumnTypeResolver
from rdl.data_sources.ChangeTrackingInfo import ChangeTrackingInfo
from rdl.data_sources.SourceTableInfo import SourceTableInfo
from rdl.shared import Providers
from rdl.shared.Utils import prevent_senstive_data_logging


class MsSqlDataSource(object):
    SOURCE_TABLE_ALIAS = "src"
    CHANGE_TABLE_ALIAS = "chg"
    MSSQL_STRING_REGEX = (
        r"mssql\+pyodbc://"
        r"(?:(?P<username>[^@/?&:]+)?:(?P<password>[^@/?&:]+)?@)?"
        r"(?P<server>[^@/?&:]*)/(?P<database>[^@/?&:]*)"
        r"\?driver=(?P<driver>[^@/?&:]*)"
        r"(?:&failover=(?P<failover>[^@/?&:]*))?"
    )

    def __init__(self, connection_string, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.connection_string = connection_string
        self.database_engine = create_engine(
            connection_string, creator=self.__create_connection_with_failover
        )
        self.column_type_resolver = ColumnTypeResolver()

    @staticmethod
    def can_handle_connection_string(connection_string):
        return (
            MsSqlDataSource.__connection_string_regex_match(connection_string)
            is not None
        )

    @staticmethod
    def get_connection_string_prefix():
        return "mssql+pyodbc://"

    def get_table_info(self, table_config, last_known_sync_version):
        columns_in_database = self.__get_table_columns(table_config)
        change_tracking_info = self.__get_change_tracking_info(
            table_config, last_known_sync_version
        )
        source_table_info = SourceTableInfo(columns_in_database, change_tracking_info)
        return source_table_info

    @prevent_senstive_data_logging
    def get_table_data_frame(
        self,
        table_config,
        columns,
        batch_config,
        batch_tracker,
        batch_key_tracker,
        full_refresh,
        change_tracking_info,
    ):
        sql = self.__build_select_statement(
            table_config,
            columns,
            batch_config,
            batch_key_tracker,
            full_refresh,
            change_tracking_info,
        )

        self.logger.debug(f"Starting read of SQL Statement: \n{sql}")
        data_frame = pandas.read_sql_query(sql, self.database_engine)
        self.logger.debug("Completed read")

        batch_tracker.extract_completed_successfully(len(data_frame))

        return data_frame

    @staticmethod
    def __connection_string_regex_match(connection_string):
        return re.match(MsSqlDataSource.MSSQL_STRING_REGEX, connection_string)

    def __create_connection_with_failover(self):
        conn_string_data = MsSqlDataSource.__connection_string_regex_match(
            self.connection_string
        )
        server = conn_string_data.group("server")
        failover = conn_string_data.group("failover")
        database = conn_string_data.group("database")
        driver = "{" + conn_string_data.group("driver").replace("+", " ") + "}"
        dsn = f"DRIVER={driver};DATABASE={database};"

        username = conn_string_data.group("username")
        password = conn_string_data.group("password")

        login_cred = "Trusted_Connection=yes;"
        if username is not None and password is not None:
            login_cred = f"UID={username};PWD={password};"

        dsn += login_cred
        self.logger.info(
            "Parsed Connection Details: "
            + f"""FAILOVER={failover}
            SERVER={server}
            DRIVER={driver}
            DATABASE={database}"""
        )
        try:
            return pyodbc.connect(dsn, server=server)
        except (
            sqlalchemy.exc.OperationalError,
            pyodbc.OperationalError,
            pyodbc.ProgrammingError,
        ) as e:
            if e.args[0] in ["08001", "HYT00", "42000"] and failover is not None:
                self.logger.warning(f"Using Failover Server: {failover}")
                return pyodbc.connect(dsn, server=failover)
            raise e

    def __get_table_columns(self, table_config):
        metadata = MetaData()
        self.logger.debug(
            f"Reading definition for source table "
            f"{table_config['schema']}.{table_config['name']}"
        )
        table = Table(
            table_config["name"],
            metadata,
            schema=table_config["schema"],
            autoload=True,
            autoload_with=self.database_engine,
        )
        return list(map(lambda column: column.name, table.columns))

    def __get_change_tracking_info(self, table_config, last_known_sync_version):

        if last_known_sync_version is None:
            last_known_sync_version = "NULL"

        # in the following we determine:
        # a) the current sync version - sourced straight up from the source db.
        # b) the last valid sync version - derived from the last known sync version and its validity based on the
        #    current state of the source data.
        # c) whether a full refresh is needed - this is a derivative of the validity of the last known sync version
        #    because if the last known sync version is no longer valid, then our target data is in-an-invalid-state /
        #    out-of-sync and a full refresh must be forced to sync the data.
        # d) whether any data has changed for the table synce the last sync - this is a derivative of the results of
        #    the CHANGETABLE() call, which we perform later and join to the table to load in the changed rows.
        #
        # the following help us determining the above:
        # a) sync_version: the current version of change tracking at source database.
        #                  it's value IS sourced from CHANGE_TRACKING_CURRENT_VERSION()
        #                  and it's value also becomes the last_known_sync_version for the next iteration.
        # b) last_known_sync_version: the tracking number of the last time we ran rdl, if we did.
        #                  it's value WAS sourced from CHANGE_TRACKING_CURRENT_VERSION().
        # c) min_valid_version: the minimum version that is valid for use in obtaining change tracking information from
        #                       the specified table.
        #                       it's value IS sourced from CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(..)).
        # d) data_changed_since_last_sync: whether or not data has changed in the table since last_known_sync_version
        #                       it's value is sourced from CHANGETABLE(table, last_known_sync_version).

        get_change_tracking_info_sql = (
            f""
            f"DECLARE @sync_version                     BIGINT  = CHANGE_TRACKING_CURRENT_VERSION(); \n"
            f"DECLARE @min_valid_version                BIGINT  ="
            f" CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('{table_config['schema']}.{table_config['name']}')); \n"
            f"DECLARE @last_known_sync_version          BIGINT  = {last_known_sync_version}; \n"
            f"DECLARE @last_known_sync_version_is_valid BIT     ="
            f" CASE WHEN @last_known_sync_version >= @min_valid_version THEN 1 ELSE 0 END; \n"
            f"DECLARE @last_sync_version                BIGINT; \n"
            f"DECLARE @force_full_load                  BIT; \n"
            f"DECLARE @data_changed_since_last_sync     BIT; \n"
            f" \n"
            f"IF @last_known_sync_version_is_valid = 1 \n"
            f"BEGIN \n"
            f"    SET @force_full_load   = 0; \n"
            f"    SET @last_sync_version = @last_known_sync_version; \n"
            f"END \n"
            f"ELSE \n"
            f"BEGIN \n"
            f"    SET @force_full_load   = 1; "
            f"    SET @last_sync_version = 0; \n"
            f"END \n"
            f" \n"
            f"IF EXISTS ( "
            f"  SELECT 1"
            f"  FROM CHANGETABLE(CHANGES "
            f"      {table_config['schema']}.{table_config['name']}, {last_known_sync_version} ) "
            f"  as c )"
            f"BEGIN \n"
            f"    SET @data_changed_since_last_sync = 1; \n"
            f"END \n"
            f"ELSE \n"
            f"BEGIN \n"
            f"    SET @data_changed_since_last_sync = 0; \n"
            f"END \n"
            f" \n"
            f"SELECT @sync_version              AS sync_version \n"
            f", @last_sync_version              AS last_sync_version \n"
            f", @force_full_load                AS force_full_load \n"
            f", @data_changed_since_last_sync   AS data_changed_since_last_sync; \n"
        )

        self.logger.debug(
            f"Getting ChangeTracking info for {table_config['schema']}.{table_config['name']}.\n"
            f"{get_change_tracking_info_sql}"
        )

        result = self.database_engine.execute(text(get_change_tracking_info_sql))
        row = result.fetchone()

        return ChangeTrackingInfo(
            row["last_sync_version"],
            row["sync_version"],
            row["force_full_load"],
            row["data_changed_since_last_sync"],
        )

    def __build_select_statement(
        self,
        table_config,
        columns,
        batch_config,
        batch_key_tracker,
        full_refresh,
        change_tracking_info,
    ):
        column_array = list(
            map(
                lambda cfg: MsSqlDataSource.prefix_column(
                    cfg["source_name"], full_refresh, table_config["primary_keys"]
                ),
                columns,
            )
        )
        column_names = ", ".join(column_array)

        if full_refresh:
            select_sql = f"SELECT TOP ({batch_config['size']}) {column_names}"
            from_sql = f"FROM {table_config['schema']}.{table_config['name']} AS {MsSqlDataSource.SOURCE_TABLE_ALIAS}"
            where_sql = f"WHERE {self.__build_where_clause(batch_key_tracker, MsSqlDataSource.SOURCE_TABLE_ALIAS)}"
            order_by_sql = (
                "ORDER BY "
                + f", {MsSqlDataSource.SOURCE_TABLE_ALIAS}.".join(
                    table_config["primary_keys"]
                )
            )
        else:
            select_sql = (
                f"SELECT TOP ({batch_config['size']}) {column_names}, "
                f"{MsSqlDataSource.CHANGE_TABLE_ALIAS}.SYS_CHANGE_VERSION"
                f" AS {Providers.AuditColumnsNames.CHANGE_VERSION}, "
                f"CASE {MsSqlDataSource.CHANGE_TABLE_ALIAS}.SYS_CHANGE_OPERATION WHEN 'D' THEN 1 ELSE 0 "
                f"END AS {Providers.AuditColumnsNames.IS_DELETED}"
            )
            from_sql = (
                f"FROM CHANGETABLE(CHANGES"
                f" {table_config['schema']}.{table_config['name']},"
                f" {change_tracking_info.last_sync_version})"
                f" AS {MsSqlDataSource.CHANGE_TABLE_ALIAS}"
                f" LEFT JOIN {table_config['schema']}.{table_config['name']} AS {MsSqlDataSource.SOURCE_TABLE_ALIAS}"
                f" ON {self.__build_change_table_on_clause(batch_key_tracker)}"
            )
            where_sql = f"WHERE {self.__build_where_clause(batch_key_tracker, MsSqlDataSource.CHANGE_TABLE_ALIAS)}"
            order_by_sql = (
                "ORDER BY "
                + f", {MsSqlDataSource.CHANGE_TABLE_ALIAS}.".join(
                    table_config["primary_keys"]
                )
            )

        return f"{select_sql} \n {from_sql} \n {where_sql} \n {order_by_sql};"

    @staticmethod
    def prefix_column(column_name, full_refresh, primary_key_column_names):
        if not isinstance(primary_key_column_names, (list, tuple)):
            raise TypeError(
                f"Argument 'primary_key_column_names' must be a list or tuple"
            )
        if column_name in primary_key_column_names and not full_refresh:
            return f"{MsSqlDataSource.CHANGE_TABLE_ALIAS}.{column_name}"
        else:
            return f"{MsSqlDataSource.SOURCE_TABLE_ALIAS}.{column_name}"

    @staticmethod
    def __build_where_clause(batch_key_tracker, table_alias):
        has_value = False

        try:
            sql_builder = io.StringIO()
            where_stack = io.StringIO()
            where_stack.write("1 = 1")
            for primary_key in batch_key_tracker.bookmarks:
                if has_value:
                    sql_builder.write(" OR")

                sql_builder.write(
                    f" ({where_stack.getvalue()} AND {table_alias}.{primary_key} > {batch_key_tracker.bookmarks[primary_key]})"
                )
                has_value = True
                where_stack.write(
                    f" AND {table_alias}.{primary_key} = {batch_key_tracker.bookmarks[primary_key]}"
                )

            return sql_builder.getvalue()
        finally:
            sql_builder.close()
            where_stack.close()

    @staticmethod
    def __build_change_table_on_clause(batch_key_tracker):
        has_value = False

        try:
            sql_builder = io.StringIO()
            for primary_key in batch_key_tracker.bookmarks:
                if has_value:
                    sql_builder.write(" AND ")

                sql_builder.write(
                    f" {MsSqlDataSource.CHANGE_TABLE_ALIAS}.{primary_key} ="
                    f" {MsSqlDataSource.SOURCE_TABLE_ALIAS}.{primary_key}"
                )
                has_value = True

            return sql_builder.getvalue()
        finally:
            sql_builder.close()
