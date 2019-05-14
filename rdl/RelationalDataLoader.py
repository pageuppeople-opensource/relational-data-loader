import logging
import argparse
from datetime import datetime
from sqlalchemy import create_engine
from rdl.DataLoadManager import DataLoadManager
from rdl.shared import Constants, Providers
from rdl.data_load_tracking.DataLoadTrackerRepository import DataLoadTrackerRepository
from rdl.data_sources.DataSourceFactory import DataSourceFactory
from sqlalchemy.orm import sessionmaker

_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']
_AUDIT_FUNCTION_OPTIONS = {
    'FULL': DataLoadTrackerRepository.get_full_refresh_since,
    'INCR': DataLoadTrackerRepository.get_only_incremental_since,
}


class RelationalDataLoader:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.data_source_factory = DataSourceFactory()

    def main(self):
        self.args = self.get_arguments()

        self.configure_root_logger(self.args.log_level)
        Providers.AuditColumnsNames.update_audit_column_prefix(self.args.audit_column_prefix)

        self.args.func()

    def execute_process_command(self):
        source_db = self.data_source_factory.create_source(self.args.source_connection_string)

        destination_db = create_engine(self.args.destination_connection_string)
        session_maker = sessionmaker(bind=destination_db)
        repository = DataLoadTrackerRepository(session_maker)

        data_load_manager = DataLoadManager(self.args.configuration_folder, source_db, destination_db, repository)
        total_rows_processed = data_load_manager.start_imports(self.args.force_full_refresh_models)

        print(total_rows_processed)

    def execute_audit_command(self):
        destination_db = create_engine(self.args.destination_connection_string)
        session_maker = sessionmaker(bind=destination_db)
        data_load_tracker_repository = DataLoadTrackerRepository(session_maker)

        last_successful_timestamp = datetime.fromisoformat(self.args.timestamp)

        results = _AUDIT_FUNCTION_OPTIONS[self.args.model_type](data_load_tracker_repository, last_successful_timestamp)

        print(" ".join(results))

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
            message = f"Invalid choice: '{log_level_string}', choose from '{_LOG_LEVEL_STRINGS}'"
            raise argparse.ArgumentTypeError(message)

        log_level_int = getattr(logging, log_level_string, logging.INFO)
        # check the logging log_level_choices have not changed from our expected values
        assert isinstance(log_level_int, int)

        return log_level_int

    def raw_connection_string_to_valid_source_connection_string(self, connection_string):
        if not self.data_source_factory.is_prefix_supported(connection_string):
            message = f"Invalid connection string. " \
                      f"Connection strings must begin with '{self.data_source_factory.get_supported_source_prefixes()}'"
            raise argparse.ArgumentTypeError(message)
        return connection_string

    def get_arguments(self):
        parser = argparse.ArgumentParser(description=Constants.APP_NAME)

        subparsers = parser.add_subparsers(title='commands', metavar='', dest='command')

        process_command_parser = subparsers.add_parser('process', help='processes load models')
        process_command_parser.set_defaults(func=self.execute_process_command)

        process_command_parser.add_argument(
            'source_connection_string',
            metavar='source-connection-string',
            type=self.raw_connection_string_to_valid_source_connection_string,
            help='The source connections string as a 64bit ODBC system dsn. Eg: mssql+pyodbc://dwsource')

        process_command_parser.add_argument(
            'destination_connection_string',
            metavar='destination-connection-string',
            help='The destination database connection string. Provide in PostgreSQL'
            ' + Psycopg format. '
            'Eg: \'postgresql+psycopg2://username:password@host:port/dbname\'')

        process_command_parser.add_argument(
            'configuration_folder',
            metavar='configuration-folder',
            help='Absolute or relative path to the models. '
            'Eg \'./models\', \'C:/path/to/models\'')

        process_command_parser.add_argument(
            '-f',
            '--force-full-refresh-models',
            nargs='?',
            const='*',
            help='Comma separated model names in the configuration folder. '
            'These models would be forcefully refreshed dropping and recreating the '
            'destination tables. All others models would only be refreshed if required '
            'as per the state of the source and destination tables. '
            'Eg \'CompoundPkTest,LargeTableTest\'. '
            'Leave blank or use glob (*) to force full refresh of all models.')

        process_command_parser.add_argument(
            '-l', '--log-level',
            default='INFO',
            type=self.log_level_string_to_int,
            nargs='?',
            help=f'Set the logging output level. {_LOG_LEVEL_STRINGS}')

        process_command_parser.add_argument(
            '-p', '--audit-column-prefix',
            default='rdl_',
            type=str,
            nargs='?',
            help=f'Set the audit column prefix, used in the destination schema. Default is \'rdl_\'. ')

        audit_command_parser = subparsers.add_parser('audit',
                                                     help='provides list of processed models since a given timestamp')
        audit_command_parser.set_defaults(func=self.execute_audit_command)

        audit_command_parser.add_argument(
            'destination_connection_string',
            metavar='destination-connection-string',
            help='The destination database connection string. Provide in PostgreSQL'
            ' + Psycopg format. '
            'Eg: \'postgresql+psycopg2://username:password@host:port/dbname\'')

        audit_command_parser.add_argument(
            'model_type',
            metavar='model-type',
            choices=_AUDIT_FUNCTION_OPTIONS.keys(),
            help='Use the command FULL to return full refresh models or the '
            'command INCR to return only the incremental models since the timestamp')

        audit_command_parser.add_argument(
            'timestamp',
            metavar='timestamp',
            help='ISO 8601 datetime with timezone (yyyy-mm-ddThh:mm:ss.nnnnnn+|-hh:mm) used to provide information '
            'on all actions since the specified date. '
            'Eg \'2019-02-14T01:55:54.123456+00:00\'. ')

        audit_command_parser.add_argument(
            '-l', '--log-level',
            default='INFO',
            type=self.log_level_string_to_int,
            nargs='?',
            help=f'Set the logging output level. {_LOG_LEVEL_STRINGS}')

        audit_command_parser.add_argument(
            '-p', '--audit-column-prefix',
            default='rdl_',
            type=str,
            nargs='?',
            help=f'Set the audit column prefix, used in the destination schema. Default is \'rdl_\'. ')

        return parser.parse_args()

    def str2bool(self, v):
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')
