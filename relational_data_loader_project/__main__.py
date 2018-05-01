import logging
from relational_data_loader_project.DataLoadManager import DataLoadManager
from sqlalchemy import create_engine
import argparse


def main(args):
    configure_logging()
    source_engine = create_engine(args['source-engine'])
    destination_engine = create_engine(args['destination-engine'])

    data_load_manager = DataLoadManager(args['configuration-folder'])
    data_load_manager.start_import(source_engine, destination_engine, True)


def configure_logging():
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger()
    console_stream_handler = logging.StreamHandler()
    console_stream_handler.setFormatter(formatter)
    log.addHandler(console_stream_handler)
    log.setLevel(logging.DEBUG)
    return

def get_arguments():
    parser = argparse.ArgumentParser(description='Relational Data Loader')

    parser.add_argument('source-engine', metavar='source-engine',
                        help='The source engine. Eg: mssql+pyodbc://dwsource')

    parser.add_argument('destination-engine', metavar='destination-engine',
                        help='The destination engine. Eg: postgresql+psycopg2://postgres:xxxx@localhost/dest_dw')

    parser.add_argument('configuration-folder', metavar='configuration-folder',
                        help='The configration folder. Eg C:\\_dev\\oscars-misc\\el-pipeline-spike\\configuraton\\')

    return vars(parser.parse_args())


if __name__ == "__main__":
    args = get_arguments()
    main(args)



