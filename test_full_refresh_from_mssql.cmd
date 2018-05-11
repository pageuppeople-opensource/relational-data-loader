py rdl.py mssql+pyodbc://(local)\SQL2016/RelationalDataLoaderIntegrationTestSource?driver=SQL+Server+Native+Client+11.0 postgresql+psycopg2://postgres:there_is_no_password_due_to_pg_trust@localhost/relational_data_loader_integration_tests .\integration_tests\mssql_source\config\ --log-level DEBUG --full-refresh yes
if %errorlevel% neq 0 exit /b %errorlevel%








