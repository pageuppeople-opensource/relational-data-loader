from setuptools import setup, find_packages
import os
import sys

def _rdl_psycopg2_name():
    # if the user chose something, use that
    # * If you cannot install a library containing pg_config, set the environemnt variable RDL_PSYCOPG2_NAME='psycopg2-binary'
    # * Reference: https://stackoverflow.com/questions/11618898/pg-config-executable-not-found/12037133#12037133
    package_name = os.getenv('RDL_PSYCOPG2_NAME', '')
    if package_name:
        return package_name

    binary_only_versions = [(3, 8)]

    # binary wheels don't exist for all versions. Require psycopg2-binary for
    # them and wait for psycopg2.
    if sys.version_info[:2] in binary_only_versions:
        return 'psycopg2-binary'
    else:
        return 'psycopg2'

RDL_PSYCOPG2_NAME = _rdl_psycopg2_name()

setup(
    name="rdl",
    version="0.1.24-beta",
    packages=find_packages(),
    install_requires=[
        "numpy==1.16.2",
        "pandas==0.24.2",
        '{}~=2.8'.format(RDL_PSYCOPG2_NAME),
        "pyodbc==4.0.26",
        "python-dateutil==2.8.0",
        "pytz==2019.1",
        "six==1.12.0",
        "SQLAlchemy==1.3.3",
        "sqlalchemy-citext==1.3.post0",
        "alembic==1.0.9",
        "boto3==1.9.224",
    ],
    package_data={"": ["alembic.ini", "alembic/*.py", "alembic/**/*.py"]},
)
