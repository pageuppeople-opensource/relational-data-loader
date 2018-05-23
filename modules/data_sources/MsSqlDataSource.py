import logging
import pandas
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.schema import Table
from modules.ColumnTypeResolver import ColumnTypeResolver

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


    def build_select_statement(self, table_configuration, columns, batch_configuration, previous_batch_key):
        column_array = list(map(lambda cfg: cfg['source_name'], columns))
        column_names = ", ".join(column_array)

        return "SELECT TOP ({0}) {1} FROM {2}.{3} WHERE {4} > {5} ORDER BY {4}".format(batch_configuration['size'],
                                                                                       column_names,
                                                                                       table_configuration[
                                                                                           'schema'],
                                                                                       table_configuration[
                                                                                           'name'],
                                                                                       table_configuration[
                                                                                           'primary_key'],
                                                                                       previous_batch_key)

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


    def get_next_data_frame(self, table_configuration, columns, batch_configuration, batch_tracker, previous_batch_key):
        sql = self.build_select_statement(table_configuration, columns, batch_configuration, previous_batch_key)
        self.logger.debug("Starting read of SQL Statement: {0}".format(sql))
        data_frame = pandas.read_sql_query(sql, self.database_engine)

        self.logger.debug("Completed read")

        batch_tracker.extract_completed_successfully(len(data_frame))

        return data_frame
