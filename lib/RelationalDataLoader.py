import logging
from lib.DataLoadManager import DataLoadManager
from lib.data_sources.DataSourceFactory import DataSourceFactory

from sqlalchemy import create_engine
import argparse

_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']


class RelationalDataLoader:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.data_source_factory = DataSourceFactory()

    def main(self):
        args = self.get_arguments()

        self.configure_logging(args['log_level'])
        data_source = self.data_source_factory.create_source(args['source-connection-string'])

        destination_engine = create_engine(args['destination-engine'])

        data_load_manager = DataLoadManager(args['configuration-folder'], data_source)
        data_load_manager.start_imports(destination_engine, args['full_refresh'])

    def configure_logging(self, log_level):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log = logging.getLogger()
        console_stream_handler = logging.StreamHandler()
        console_stream_handler.setFormatter(formatter)
        log.addHandler(console_stream_handler)
        log.setLevel(log_level)
        return

    def log_level_string_to_int(self, log_level_string):
        if not log_level_string in _LOG_LEVEL_STRINGS:
            message = 'invalid choice: {0} (choose from {1})'.format(log_level_string, _LOG_LEVEL_STRINGS)
            raise argparse.ArgumentTypeError(message)

        log_level_int = getattr(logging, log_level_string, logging.INFO)
        # check the logging log_level_choices have not changed from our expected values
        assert isinstance(log_level_int, int)

        return log_level_int

    def raw_connection_string_to_valid_source_connection_string(self, connection_string):
        if not self.data_source_factory.is_prefix_supported(connection_string):
            message = 'invalid connection string: {0} (connection strings must begin with {1})'.format(connection_string, self.data_source_factory.get_supported_source_prefixes())
            raise argparse.ArgumentTypeError(message)
        return connection_string

    def get_arguments(self):
        parser = argparse.ArgumentParser(description='Relational Data Loader')

        parser.add_argument('source-connection-string', metavar='source-connection-string',
                            type=self.raw_connection_string_to_valid_source_connection_string,
                            help='The source connections string. Eg: mssql+pyodbc://dwsource or '
                                 'csv://c://some//Path//To//Csv//Files//')

        parser.add_argument('destination-engine', metavar='destination-engine',
                            help='The destination engine. Eg: postgresql+psycopg2://postgres:xxxx@localhost/dest_dw')

        parser.add_argument('configuration-folder', metavar='configuration-folder',
                            help='The configuration folder. Eg C:\\_dev\\oscars-misc\\el-pipeline-spike\\configuration\\')

        parser.add_argument('--log-level',
                            default='INFO',
                            type=self.log_level_string_to_int,
                            nargs='?',
                            help='Set the logging output level. {0}'.format(_LOG_LEVEL_STRINGS))

        parser.add_argument("--full-refresh", type=self.str2bool, nargs='?',
                            default=False,
                            help='If true, a full refresh of the destination will be performed. This drops/re-creates '
                                 'the destination table(s).')


        return vars(parser.parse_args())

    def str2bool(self, v):
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')
