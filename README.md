# RelationalDataLoader

## About

A utility for taking data from MS-SQL and loading it into PostgreSQL

## Usage

`py rdl.py --help`

```text
usage: rdl.py [-h] [-m [FORCE_FULL_REFRESH_MODELS]] [-l [LOG_LEVEL]]
              source-connection-string destination-connection-string
              configuration-folder

Relational Data Loader

positional arguments:
  source-connection-string
                        The source connections string as a 64bit ODBC system
                        dsn. Eg: mssql+pyodbc://dwsource or
                        csv://c://some//Path//To//Csv//Files//
  destination-connection-string
                        The destination database connection string. Provide in
                        PostgreSQL + Psycopg format. Eg: 'postgresql+psycopg2:
                        //username:password@host:port/dbname'
  configuration-folder  Absolute or relative path to the models. Eg
                        './models', 'C:/path/to/models'

optional arguments:
  -h, --help            show this help message and exit
  -m [FORCE_FULL_REFRESH_MODELS], --force-full-refresh-models [FORCE_FULL_REFRESH_MODELS]
                        Comma separated model names in the configuration
                        folder. These models would be forcefully refreshed
                        dropping and recreating the destination tables. All
                        others models would only be refreshed if required as
                        per the state of the source and destination tables.Eg
                        'CompoundPkTest,LargeTableTest'. Use glob (*) to force
                        full refresh of all models.
  -l [LOG_LEVEL], --log-level [LOG_LEVEL]
                        Set the logging output level. ['CRITICAL', 'ERROR',
                        'WARNING', 'INFO', 'DEBUG']
```

_Notes:_

- `destination-connection-string` is a [PostgreSQL Db Connection String](http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2)

### Examples

See `test_*.cmd` scripts for usage samples.

## Development

### Testing

#### Integration

The test batch files assume there is a user by the name of `postgres` on the system.
It also sends through a nonense password - it is assumed that the target system is running in 'trust' mode.
_See [Postgres docs](https://www.postgresql.org/docs/9.1/static/auth-pg-hba-conf.html) for details on trust mode._

#### Unit

_Setup:_

Create a new SQL Server Login/User using the script below. Make sure you update it with your desired password and if you update the username / login, then also sync the same with the `modules\tests\config\connection.json` file.

```sql
USE master;
GO
IF NOT EXISTS(SELECT * FROM sys.syslogins WHERE NAME = 'rdl_test_user')
    CREATE LOGIN [rdl_test_user] WITH PASSWORD=N'hunter2', CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;
GO
ALTER SERVER ROLE [dbcreator] ADD MEMBER [rdl_test_user]
GO
```

_Execution:_

Execution is as simply as `python3 run_tests.py`

### `Destination.Type` Values

The destination.type value controls both the data reader type and the destination column type. These are implemented in Column_Type_Resolver.py.

They are mapped as follows:

| destination.type            | pandas type | sqlalchemy type                      | dw column type | notes                                                                  |
| --------------------------- | ----------- | ------------------------------------ | -------------- | ---------------------------------------------------------------------- |
| string                      | str         | citext.CIText                        | citext         | A case-insensitive string that supports unicode                        |
| int (when nullable = false) | int         | sqlalchemy.Integer                   | int            | An (optionally) signed INT value                                       |
| int (when nullable = true)  | object      | sqlalchemy.Integer                   | int            | An (optionally) signed INT value                                       |
| datetime                    | str         | sqlalchemy.DateTime                  | datetime (tz?) |                                                                        |
| json                        | str         | sqlalchemy.dialects.postgresql.JSONB | jsonb          | Stored as binary-encoded json on the database                          |
| numeric                     | float       | sqlalchemy.Numeric                   | numeric        | Stores whole and decimal numbers                                       |
| guid                        | str         | sqlalchemy.dialects.postgresql.UUID  | uuid           |                                                                        |
| bigint                      | int         | sqlalchemy.BigInteger                | BigInt         | Relies on 64big python. Limited to largest number of ~2147483647121212 |
| boolean                     | bool        | sqlalchemy.Boolean                   | Boolean        |                                                                        |
