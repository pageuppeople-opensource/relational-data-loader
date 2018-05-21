import citext
from sqlalchemy import MetaData, DateTime, Numeric, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
class ColumnTypeResolver(object):
    PANDAS_TYPE_MAP = {'string': str,
                       'datetime': str,
                       'json': str,
                       'numeric': float,
                       'guid': str}

    POSTGRES_TYPE_MAP = {'string': citext.CIText,
                         'datetime': DateTime,
                         'json': JSONB,
                         'numeric': Numeric,
                         'guid': UUID,
                         'int': Integer
                         }

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
