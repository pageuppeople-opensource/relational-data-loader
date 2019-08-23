import logging
from rdl.data_sources.MsSqlDataSource import MsSqlDataSource
from rdl.data_sources.AWSLambdaDataSource import AWSLambdaDataSource


class DataSourceFactory(object):
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.sources = [MsSqlDataSource, AWSLambdaDataSource]

    def create_source(self, connection_string):
        for source in self.sources:
            if source.can_handle_connection_string(connection_string):
                self.logger.info(
                    f"Found handler '{source}' for given connection string."
                )
                return source(connection_string)

        raise RuntimeError(
            "There are no data sources that can handle this connection string"
        )

    def is_prefix_supported(self, connection_string):
        for source in self.sources:
            if source.can_handle_connection_string(connection_string):
                return True
        return False

    def get_supported_source_prefixes(self):
        return list(
            map(lambda source: source.get_connection_string_prefix(), self.sources)
        )
