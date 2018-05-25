from setuptools import setup, find_packages
setup(name='rdl',
      version='0.1',
      packages = find_packages(),
      install_requires=[
        'numpy==1.14.2',
        'pandas==0.22.0',
        'psycopg2==2.7.4',
        'pyodbc==4.0.23',
        'python-dateutil==2.7.2',
        'pytz==2018.4',
        'six==1.11.0',
        'SQLAlchemy==1.2.7',
        'sqlalchemy-citext==1.3.post0'
    ]
      )

















