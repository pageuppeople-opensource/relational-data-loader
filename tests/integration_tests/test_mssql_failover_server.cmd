@echo off
py -m rdl process "mssql+pyodbc://(local)\SQL2016/RDL_Integration_Test_Source_Db?driver=SQL+Server+Native+Client+11.0" postgresql+psycopg2://rdl_integration_test_user:rdl_integration_test_password@localhost/rdl_integration_test_target_db ./tests/integration_tests/mssql_source/config/ --log-level DEBUG
if %errorlevel% neq 0 exit /b %errorlevel%

py -m rdl process "mssql+pyodbc://fake_server\SQL2016/RDL_Integration_Test_Source_Db?driver=SQL+Server+Native+Client+11.0&failover=(local)" postgresql+psycopg2://rdl_integration_test_user:rdl_integration_test_password@localhost/rdl_integration_test_target_db ./tests/integration_tests/mssql_source/config/ --log-level DEBUG
if %errorlevel% neq 0 exit /b %errorlevel%

py -m rdl process "mssql+pyodbc://(local)\SQL2016/RDL_Integration_Test_Source_Db?driver=SQL+Server+Native+Client+11.0&failover=fake_server" postgresql+psycopg2://rdl_integration_test_user:rdl_integration_test_password@localhost/rdl_integration_test_target_db ./tests/integration_tests/mssql_source/config/ --log-level DEBUG
if %errorlevel% neq 0 exit /b %errorlevel%
