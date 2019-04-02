@echo off
py -m rdl process "mssql+pyodbc://(local)/RelationalDataLoaderIntegrationTestSource?driver=SQL+Server+Native+Client+11.0"^
 postgresql+psycopg2://postgres:postgres@localhost/postgres^
 ./integration_tests/mssql_source/config_fail/ --log-level DEBUG 2>&1 | find "Re-run with --log-level DEBUG to override" 2>&1 1>nul
if %errorlevel% neq 1 exit /b 1

py -m rdl process "mssql+pyodbc://(local)/RelationalDataLoaderIntegrationTestSource?driver=SQL+Server+Native+Client+11.0"^
 postgresql+psycopg2://postgres:postgres@localhost/postgres^
 ./integration_tests/mssql_source/config_fail/ --log-level INFO 2>&1 | find "Re-run with --log-level DEBUG to override" 2>&1 1>nul
if %errorlevel% neq 0 exit /b %errorlevel%
