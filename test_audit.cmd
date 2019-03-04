py -m rdl audit postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@localhost/relational_data_loader_integration_tests FULL 2019-02-14T01:55:54.123456+00:00 --log-level INFO
if %errorlevel% neq 0 exit /b %errorlevel%

py -m rdl audit postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@localhost/relational_data_loader_integration_tests INCR 2019-02-14T01:55:54.123456+00:00 --log-level INFO
if %errorlevel% neq 0 exit /b %errorlevel%
