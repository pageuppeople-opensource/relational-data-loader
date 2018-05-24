# RelationalDataLoader
## About
A utility for taking data from MS-SQL and loading it into PostgreSQL


## Usage
Execute  `py rdl.py SOURCE DESTINATION CONFIGURATION-FOLDER [log-level] [full-refresh]`

Where `SOURCE` takes the following formats
**CSV:**  `csv://.\test_data\full_refresh`
**MSSQL:**  `mssql+pyodbc://dwsource`

In the above example, dwsource is a 64bit ODBC system dsn


`DESTINATION` takes the following format
**PostgreSQL:**  `postgresql+psycopg2://postgres:xxxx@localhost/dest_dw`


### Examples
#### CSV Source

`py rdl.py csv://.\test_data\full_refresh postgresql+psycopg2://postgres:xxxx@localhost/dest_dw .\configuration\ --log-level INFO --full-refresh yes`
`py rdl.py csv://.\test_data\incremental_refresh postgresql+psycopg2://postgres:xxxx@localhost/dest_dw .\configuration\ --log-level INFO --full-refresh no`


#### MSSQL Source



### Troubleshooting
Run with  `--log-level DEBUG` on the command line.


## Other Notes
### Testing
The test batch files assume there is a user by the name of `postgres` on the system.
It also sends through a nonense password - it is assumed that the target system is running in 'trust' mode.
See https://www.postgresql.org/docs/9.1/static/auth-pg-hba-conf.html for details on trust mode



###Destination.Type Values
The destination.type value controls both the data reader type and the destination column type. They are mapped as followed

| destination.type            | pandas type | sqlalchemy type                       | dw column type | notes                                            |
|-----------------------------|-------------|---------------------------------------|----------------|--------------------------------------------------|
| string                      | str         | citext.CIText                         | citext         | A case-insensitive string that supports unicode  |
| int (when nullable = false) | int         | sqlalchemy.Integer                    | int            | An (optionally) signed INT value                 |  
| int (when nullable = true)  | object      | sqlalchemy.Integer                    | int            | An (optionally) signed INT value                 |  
| datetime                    | str         | sqlalchemy.DateTime                   | datetime (tz?) |                                                  | 
| json                        | str         | sqlalchemy.dialects.postgresql.JSONB  | jsonb          | Stored as binary-encoded json on the database    |
| numeric                     | float       | sqlalchemy.Numeric                    | numeric        | Stores whole and decimal numbers                 |
| guid                        | str         | sqlalchemy.dialects.postgresql.UUID   | uuid           | |
| bigint                      | int         | sqlalchemy.BigInteger                 | BigInt         | Relies on 64big python. Limited to largest number of ~2147483647121212|


These are implemented in Column_Type_Resolver.py


                   

                   


