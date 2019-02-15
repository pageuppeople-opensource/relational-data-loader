chcp 65001
echo {"username": "","password": "","server_string": "(local)\\SQL2016"} > .\modules\tests\config\connection.json

py ./run_tests.py
if %errorlevel% neq 0 exit /b %errorlevel%
