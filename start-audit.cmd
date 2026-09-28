@echo off
setlocal
rem Keep the caller's working directory; only resolve the script and log paths.
set "AUDIT_SCRIPT=%~dp0omp_audit_proxy.py"
set "AUDIT_LOG_DIR=%~dp0.."
if defined OMP_AUDIT_LOG_DIR set "AUDIT_LOG_DIR=%OMP_AUDIT_LOG_DIR%"
if not exist "%AUDIT_SCRIPT%" (
    echo [audit] Cannot find omp_audit_proxy.py beside this launcher. 1>&2
    exit /b 2
)
python -B -u -X utf8 "%AUDIT_SCRIPT%" --log "%AUDIT_LOG_DIR%\audit.jsonl" %*
set "AUDIT_EXIT=%ERRORLEVEL%"
endlocal & exit /b %AUDIT_EXIT%
