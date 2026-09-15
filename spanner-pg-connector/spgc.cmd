@echo off

call "%~dp0spanner-pg-connector.cmd" %*
exit /b %ERRORLEVEL%
