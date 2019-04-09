@echo off
py -m rdl process csv://./tests/integration_tests/csv_source/full_refresh_data/ postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@/relational_data_loader_integration_tests ./tests/integration_tests/csv_source/config/ --log-level INFO --force-full-refresh-models *
if %errorlevel% neq 0 exit /b %errorlevel%

psql -U postgres -d relational_data_loader_integration_tests -a -v ON_ERROR_STOP=1 -f ./tests/integration_tests/csv_source/assertions/column_test_full_refresh_assertions.sql
if %errorlevel% neq 0 exit /b %errorlevel%