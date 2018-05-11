py rdl.py csv://.\integration_tests\csv_source\full_refresh_data\ postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@/relational_data_loader_integration_tests .\integration_tests\csv_source\config\ --log-level INFO --full-refresh yes
if %errorlevel% neq 0 exit /b %errorlevel%

py rdl.py csv://.\integration_tests\csv_source\incremental_refresh_data\ postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@localhost/relational_data_loader_integration_tests .\integration_tests\csv_source\config\ --log-level INFO --full-refresh no
if %errorlevel% neq 0 exit /b %errorlevel%


psql -U postgres -q -d relational_data_loader_integration_tests -a -v ON_ERROR_STOP=1 -f .\integration_tests\csv_source\assertions\column_test_incremental_refresh_assertions.sql
if %errorlevel% neq 0 exit /b %errorlevel%