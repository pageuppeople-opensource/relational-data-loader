from sqlalchemy import DateTime, Numeric, Integer, BigInteger, Boolean, String
from sqlalchemy.dialects.postgresql import JSONB


class ColumnTypeResolver(object):
    PANDAS_TYPE_MAP = {
        "string": str,
        "datetime": str,
        "json": str,
        "numeric": float,
        "guid": str,
        "bigint": int,
        "boolean": bool,
    }

    POSTGRES_TYPE_MAP = {
        "string": String(length=65535),
        "datetime": DateTime,
        "json": JSONB,
        "numeric": Numeric,
        "guid": String,
        "int": Integer,
        "bigint": BigInteger,
        "boolean": Boolean,
    }

    def resolve_postgres_type(self, column):
        return self.POSTGRES_TYPE_MAP[column["type"]]

    def resolve_pandas_type(self, column):
        if column["type"] == "int":
            if column["nullable"]:
                return object
            else:
                return int
        else:
            return self.PANDAS_TYPE_MAP[column["type"]]

    def create_column_type_dictionary(self, columns):
        types = {}
        for column in columns:
            types[column["source_name"]] = self.resolve_pandas_type(
                column["destination"]
            )
        return types
