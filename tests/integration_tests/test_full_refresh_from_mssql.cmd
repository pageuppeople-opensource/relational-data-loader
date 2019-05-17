@echo off
py -m rdl process mssql+pyodbc://(local)\SQL2016/RDL_Integration_Test_Source_Db?driver=SQL+Server+Native+Client+11.0 postgresql+psycopg2://rdl_integration_test_user:rdl_integration_test_password@localhost/rdl_integration_test_target_db ./tests/integration_tests/mssql_source/config/ --log-level DEBUG --force-full-refresh-models *
if %errorlevel% neq 0 exit /b %errorlevel%

py -m rdl process mssql+pyodbc://(local)\SQL2016/RDL_Integration_Test_Source_Db?driver=SQL+Server+Native+Client+11.0 postgresql+psycopg2://rdl_integration_test_user:rdl_integration_test_password@localhost/rdl_integration_test_target_db ./tests/integration_tests/mssql_source/config/ --log-level DEBUG --force-full-refresh-models=CompoundPkTest
if %errorlevel% neq 0 exit /b %errorlevel%

psql -U postgres -d rdl_integration_test_target_db -a -v ON_ERROR_STOP=1 -f ./tests/integration_tests/mssql_source/assertions/large_table_test_full_refresh_assertions.sql
if %errorlevel% neq 0 exit /b %errorlevel%
