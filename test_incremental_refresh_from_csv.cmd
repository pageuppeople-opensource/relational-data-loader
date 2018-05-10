py rdl.py csv://.\integration_tests\csv_source\full_refresh_data\ postgresql+psycopg2://postgres:password@/relational_data_loader_integration_tests .\integration_tests\csv_source\config\ --log-level INFO --full-refresh yes
if %errorlevel% neq 0 exit /b %errorlevel%

py rdl.py csv://.\integration_tests\csv_source\incremental_refresh_data\ postgresql+psycopg2://postgres:xxxx@localhost/relational_data_loader_integration_tests .\integration_tests\csv_source\config\ --log-level INFO --full-refresh no
if %errorlevel% neq 0 exit /b %errorlevel%


psql -U postgres -q -d relational_data_loader_integration_tests -a -f .\integration_tests\csv_source\assertions\column_test_incremental_refresh_assertions.sql
if %errorlevel% neq 0 exit /b %errorlevel%