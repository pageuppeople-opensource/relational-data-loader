from __future__ import with_statement

from logging.config import fileConfig

from sqlalchemy import engine_from_config, create_engine
from sqlalchemy import pool

from alembic import context
from rdl.entities import Base
from rdl.shared import Constants

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

if not context.get_x_argument():
    raise AttributeError(
        "example usage `alembic -c rdl/alembic.ini -x redshift+psycopg2://postgres:postgres@localhost/postgres downgrade -1`"
    )

url = context.get_x_argument()[0]


def use_schema(object, name, type_, reflected, compare_to):
    if (
        type_ == "table"
        and object.schema != Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME
    ):
        return False
    if (
        type_ == "column"
        and not reflected
        and object.info.get("skip_autogenerate", False)
    ):
        return False
    if type_ == "table" and name == "alembic_version":
        return False
    return True


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        include_schemas=True,
        include_object=use_schema,
        version_table=f"alembic_version_{Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}",
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = create_engine(url, poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_object=use_schema,
            version_table=f"alembic_version_{Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}",
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
