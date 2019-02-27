@echo off
py rdl.py process mssql+pyodbc://(local)\SQL2016/RelationalDataLoaderIntegrationTestSource?driver=SQL+Server+Native+Client+11.0 postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@localhost/relational_data_loader_integration_tests ./integration_tests/mssql_source/config/ --log-level DEBUG --force-full-refresh-models *
if %errorlevel% neq 0 exit /b %errorlevel%

py rdl.py process mssql+pyodbc://(local)\SQL2016/RelationalDataLoaderIntegrationTestSource?driver=SQL+Server+Native+Client+11.0 postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@localhost/relational_data_loader_integration_tests ./integration_tests/mssql_source/config/ --log-level DEBUG --force-full-refresh-models=CompoundPkTest
if %errorlevel% neq 0 exit /b %errorlevel%

psql -U postgres -d relational_data_loader_integration_tests -a -v ON_ERROR_STOP=1 -f ./integration_tests/mssql_source/assertions/large_table_test_full_refresh_assertions.sql
if %errorlevel% neq 0 exit /b %errorlevel%
