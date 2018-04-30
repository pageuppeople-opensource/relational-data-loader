import logging
from sqlalchemy import MetaData
from sqlalchemy.schema import Table



class SourceTableManager(object):
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def get_columns(self, table_configuration, source_engine):
        metadata = MetaData()
        self.logger.debug("Reading definition for source table {0}.{1}".format(table_configuration['schema'], table_configuration['name']))
        table = Table(table_configuration['name'], metadata, schema=table_configuration['schema'], autoload=True, autoload_with=source_engine)
        return list(map(lambda column:column.name, table.columns))



