import logging
from relational_data_loader_project.DataLoadManager import DataLoadManager
from relational_data_loader_project.MsSqlDataSource import MsSqlDataSource
from sqlalchemy import create_engine
import argparse

_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']


def main(args):

    configure_logging(args['log_level'])
    data_source = MsSqlDataSource(args['source-connection-string'])

    destination_engine = create_engine(args['destination-engine'])

    data_load_manager = DataLoadManager(args['configuration-folder'], data_source)
    data_load_manager.start_imports(destination_engine, True)


def configure_logging(log_level):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger()
    console_stream_handler = logging.StreamHandler()
    console_stream_handler.setFormatter(formatter)
    log.addHandler(console_stream_handler)
    log.setLevel(log_level)
    return


def _log_level_string_to_int(log_level_string):
    if not log_level_string in _LOG_LEVEL_STRINGS:
        message = 'invalid choice: {0} (choose from {1})'.format(log_level_string, _LOG_LEVEL_STRINGS)
        raise argparse.ArgumentTypeError(message)

    log_level_int = getattr(logging, log_level_string, logging.INFO)
    # check the logging log_level_choices have not changed from our expected values
    assert isinstance(log_level_int, int)

    return log_level_int


def get_arguments():
    parser = argparse.ArgumentParser(description='Relational Data Loader')

    parser.add_argument('source-connection-string', metavar='source-connection-string',
                        help='The source connections string. Eg: mssql+pyodbc://dwsource or csv://c://some//Path//To//Csv//Files//')

    parser.add_argument('destination-engine', metavar='destination-engine',
                        help='The destination engine. Eg: postgresql+psycopg2://postgres:xxxx@localhost/dest_dw')

    parser.add_argument('configuration-folder', metavar='configuration-folder',
                        help='The configuration folder. Eg C:\\_dev\\oscars-misc\\el-pipeline-spike\\configuraton\\')

    parser.add_argument('--log-level',
                        default='INFO',
                        type=_log_level_string_to_int,
                        nargs='?',
                        help='Set the logging output level. {0}'.format(_LOG_LEVEL_STRINGS))

    return vars(parser.parse_args())


if __name__ == "__main__":
    args = get_arguments()
    main(args)
