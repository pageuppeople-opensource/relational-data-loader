from setuptools import setup, find_packages
setup(name='rdl',
      version='0.1',
      packages = find_packages(),
      install_requires=[
        'numpy==1.14.2',
        'pytz==2017.2',
        'python-dateutil==2.7.2',
        'psycopg2==2.7.1',
        'pandas==0.22.0',
        'pyodbc==4.0.23',
        'six==1.11.0',
        'SQLAlchemy==1.2.7',
        'sqlalchemy-citext==1.3.post0'
    ]
      )













