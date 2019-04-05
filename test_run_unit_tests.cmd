@echo off
chcp 65001
echo {"mssql": {"username": "","password": "","server_string": "(local)\\SQL2016"}, "psql": {"username": "postgres","password": "there_is_no_password_due_to_pg_trust","server_string": "localhost"}}> .\tests\unit_tests\config\connection.json

py ./run_tests.py
if %errorlevel% neq 0 exit /b %errorlevel%
