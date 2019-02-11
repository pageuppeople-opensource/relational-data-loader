# RelationalDataLoader

## About

A utility for taking data from MS-SQL and loading it into PostgreSQL

## Usage

`py rdl.py --help`

```text
usage: rdl.py [-h] [-m [MODEL_NAMES]] [-l [LOG_LEVEL]]
              [-f [FORCE_FULL_REFRESH]]
              source-connection-string destination-connection-string
              configuration-folder

Relational Data Loader

positional arguments:
  source-connection-string
                        The source connections string. Eg:
                        mssql+pyodbc://dwsource or
                        csv://c://some//Path//To//Csv//Files//
  destination-connection-string
                        The destination database connection string. Provide in
                        PostgreSQL + Psycopg format. Eg: 'postgresql+psycopg2:
                        //username:password@host:port/dbname'
  configuration-folder  The configuration folder. Eg 'C:\_dev\oscars-misc\el-
                        pipeline-spike\configuration\'

optional arguments:
  -h, --help            show this help message and exit
  -m [MODEL_NAMES], --model-names [MODEL_NAMES]
                        Comma separated model names in the configuration
                        folder. Eg 'CompoundPkTest,LargeTableTest'. Skip
                        parameter or use glob (*) to action all files in the
                        folder.
  -l [LOG_LEVEL], --log-level [LOG_LEVEL]
                        Set the logging output level. ['CRITICAL', 'ERROR',
                        'WARNING', 'INFO', 'DEBUG']
  -f [FORCE_FULL_REFRESH], --force-full-refresh [FORCE_FULL_REFRESH]
                        If true, a full refresh of the destination will be
                        performed. This drops/re-creates the destination
                        table(s). If false, a full refresh will only be
                        performed if required as per the state of source and
                        destination databases.
```

Where `SOURCE` takes the following formats

**CSV:** `csv://.\test_data\full_refresh`

**MSSQL:** `mssql+pyodbc://dwsource`

In the above example, dwsource is a 64bit ODBC system dsn

`DESTINATION` is a [PostgreSQL Db Connection String](http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2)

**PostgreSQL:** `postgresql+psycopg2://username:password@host/dbname`

### Examples

See `test_*.cmd` scripts for usage samples.

### Troubleshooting

Run with `--log-level DEBUG` on the command line.

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

----

### `Destination.Type` Values

The destination.type value controls both the data reader type and the destination column type. They are mapped as followed

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

These are implemented in Column_Type_Resolver.py
