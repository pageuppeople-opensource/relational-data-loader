from setuptools import setup, find_packages

setup(name='rdl',
      version='0.1.5',
      packages=find_packages(),
      install_requires=[
          'numpy==1.16.1',
          'pandas==0.24.1',
          'psycopg2==2.7.7',
          'pyodbc==4.0.25',
          'python-dateutil==2.8.0',
          'pytz==2018.9',
          'six==1.12.0',
          'SQLAlchemy==1.2.17',
          'sqlalchemy-citext==1.3.post0',
          'alembic==1.0.8',
      ],
      package_data={
          '': ['alembic.ini', 'alembic/*.py', 'alembic/**/*.py'],
      },
      )
