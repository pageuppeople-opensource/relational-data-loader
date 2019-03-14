@echo off
py -m rdl process "mssql+pyodbc://(local)\SQL2016/RelationalDataLoaderIntegrationTestSource?driver=SQL+Server+Native+Client+11.0" postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@localhost/relational_data_loader_integration_tests ./integration_tests/mssql_source/config/ --log-level DEBUG
if %errorlevel% neq 0 exit /b %errorlevel%

py -m rdl process "mssql+pyodbc://fake_server\SQL2016/RelationalDataLoaderIntegrationTestSource?driver=SQL+Server+Native+Client+11.0&failover=(local)" postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@localhost/relational_data_loader_integration_tests ./integration_tests/mssql_source/config/ --log-level DEBUG
if %errorlevel% neq 0 exit /b %errorlevel%

py -m rdl process "mssql+pyodbc://(local)\SQL2016/RelationalDataLoaderIntegrationTestSource?driver=SQL+Server+Native+Client+11.0&failover=fake_server" postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@localhost/relational_data_loader_integration_tests ./integration_tests/mssql_source/config/ --log-level DEBUG
if %errorlevel% neq 0 exit /b %errorlevel%
