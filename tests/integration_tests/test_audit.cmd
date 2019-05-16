@echo off
py -m rdl audit postgresql+psycopg2://rdl_integration_test_user:rdl_integration_test_password@localhost/rdl_integration_test_target_db FULL 2019-02-14T01:55:54.123456+00:00 --log-level INFO
if %errorlevel% neq 0 exit /b %errorlevel%

py -m rdl audit postgresql+psycopg2://rdl_integration_test_user:rdl_integration_test_password@localhost/rdl_integration_test_target_db INCR 2019-02-14T01:55:54.123456+00:00 --log-level INFO
if %errorlevel% neq 0 exit /b %errorlevel%
