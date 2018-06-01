import citext
from sqlalchemy import DateTime, Numeric, Integer, BigInteger
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID


class ColumnTypeResolver(object):
    PANDAS_TYPE_MAP = {'string': str,
                       'datetime': str,
                       'json': str,
                       'numeric': float,
                       'guid': str,
                       'bigint': int}

    POSTGRES_TYPE_MAP = {'string': citext.CIText,
                         'datetime': DateTime,
                         'json': JSONB,
                         'numeric': Numeric,
                         'guid': UUID,
                         'int': Integer,
                         'bigint': BigInteger}

    def resolve_postgres_type(self, column):
        return self.POSTGRES_TYPE_MAP[column['type']]

    def resolve_pandas_type(self, column):
        if column['type'] == 'int':
            if column['nullable']:
                return object
            else:
                return int
        else:
            return self.PANDAS_TYPE_MAP[column['type']]

    def create_column_type_dictionary(self, columns):
        types = {}
        for column in columns:
            types[column['source_name']] = self.resolve_pandas_type(column['destination'])
        return types
