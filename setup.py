from setuptools import setup, find_packages

setup(name='rdl',
      version='0.1.5',
      packages=find_packages(),
      install_requires=[
          'numpy==1.16.2',
          'pandas==0.24.2',
          'psycopg2-binary==2.8.2',
          'pyodbc==4.0.26',
          'python-dateutil==2.8.0',
          'pytz==2019.1',
          'six==1.12.0',
          'SQLAlchemy==1.3.3',
          'sqlalchemy-citext==1.3.post0',
          'alembic==1.0.9',
      ],
      package_data={
          '': ['alembic.ini', 'alembic/*.py', 'alembic/**/*.py'],
      },
      )
