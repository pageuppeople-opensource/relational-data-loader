import logging
import argparse
from sqlalchemy import create_engine
from modules.DataLoadManager import DataLoadManager
from modules.data_load_tracking.DataLoadTrackerRepository import DataLoadTrackerRepository
from modules.data_sources.DataSourceFactory import DataSourceFactory
from sqlalchemy.orm import sessionmaker

_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']


class RelationalDataLoader:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.data_source_factory = DataSourceFactory()

    def main(self):
        args = self.get_arguments()

        self.configure_root_logger(args.log_level)
        source_db = self.data_source_factory.create_source(args.source_connection_string)

        destination_db = create_engine(args.destination_connection_string)

        session_maker = sessionmaker(bind=destination_db)
        repository = DataLoadTrackerRepository(session_maker)
        repository.create_tables(destination_db)
        data_load_manager = DataLoadManager(args.configuration_folder, source_db, repository)
        data_load_manager.start_imports(destination_db, args.force_full_refresh_models)

    def configure_root_logger(self, log_level):
        # get the root logger
        logger = logging.getLogger()

        # set the given log level
        logger.setLevel(log_level)

        # add one handler, at the same log level, with appropriate formatting
        console_stream_handler = logging.StreamHandler()
        console_stream_handler.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_stream_handler.setFormatter(formatter)
        logger.addHandler(console_stream_handler)

        return

    def log_level_string_to_int(self, log_level_string):
        if log_level_string not in _LOG_LEVEL_STRINGS:
            message = 'invalid choice: {0} (choose from {1})'.format(log_level_string, _LOG_LEVEL_STRINGS)
            raise argparse.ArgumentTypeError(message)

        log_level_int = getattr(logging, log_level_string, logging.INFO)
        # check the logging log_level_choices have not changed from our expected values
        assert isinstance(log_level_int, int)

        return log_level_int

    def raw_connection_string_to_valid_source_connection_string(self, connection_string):
        if not self.data_source_factory.is_prefix_supported(connection_string):
            message = 'invalid connection string: {0} (connection strings must begin with {1})'.format(
                connection_string, self.data_source_factory.get_supported_source_prefixes())
            raise argparse.ArgumentTypeError(message)
        return connection_string

    def get_arguments(self):
        parser = argparse.ArgumentParser(description='Relational Data Loader')

        parser.add_argument(
            'source_connection_string',
            metavar='source-connection-string',
            type=self.raw_connection_string_to_valid_source_connection_string,
            help='The source connections string as a 64bit ODBC system dsn. Eg: mssql+pyodbc://dwsource or '
            'csv://c://some//Path//To//Csv//Files//')

        parser.add_argument('destination_connection_string',
                            metavar='destination-connection-string',
                            help='The destination database connection string. Provide in PostgreSQL + Psycopg format. '
                                 'Eg: \'postgresql+psycopg2://username:password@host:port/dbname\'')

        parser.add_argument('configuration_folder',
                            metavar='configuration-folder',
                            help='Absolute or relative path to the models. '
                                 'Eg \'./models\', \'C:/path/to/models\'')

        parser.add_argument('-f',
                            '--force-full-refresh-models',
                            nargs='?',
                            const='*',
                            help='Comma separated model names in the configuration folder. These models would be '
                                 'forcefully refreshed dropping and recreating the destination tables. All others '
                                 'models would only be refreshed if required as per the state of the source and '
                                 'destination tables. '
                                 'Eg \'CompoundPkTest,LargeTableTest\'. '
                                 'Leave blank or use glob (*) to force full refresh of all models.')

        parser.add_argument('-l', '--log-level',
                            default='INFO',
                            type=self.log_level_string_to_int,
                            nargs='?',
                            help='Set the logging output level. {0}'.format(_LOG_LEVEL_STRINGS))

        return parser.parse_args()

    def str2bool(self, v):
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')
