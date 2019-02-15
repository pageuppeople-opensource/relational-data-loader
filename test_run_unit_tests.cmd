chcp 65001
echo {"username": "","password": "","server_string": "(local)\\SQL2016"} > .\modules\tests\config\connection.json

py ./run_tests.py
if ERRORLEVEL 0 echo Does zero print?
if ERRORLEVEL 1 echo Does one print?
if %ERRORLEVEL% 0 echo Does percentage zero print?
if %ERRORLEVEL% 1 echo Does percentage one print?
if %errorlevel% neq 0 exit /b %errorlevel%
