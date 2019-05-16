@echo off
chcp 65001
echo {"mssql": {"username": "","password": "","server_string": "(local)\\SQL2016"}, "psql": {"username": "rdl_integration_test_user","password": "rdl_integration_test_password","server_string": "localhost"}}> .\tests\unit_tests\config\connection.json

py ./tests/unit_tests/main.py
if %errorlevel% neq 0 exit /b %errorlevel%
