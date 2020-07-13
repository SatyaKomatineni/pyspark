@echo off
@rem batch file to change directories to spark related directories

if "%1" == "1" goto none

if "%1" == "2" goto sparkexamples

goto none

:none
cd C:\satya\data\code\pyspark
goto done

:sparkexamples
cd c:\satya\i\spark\examples\src\main\python
goto done

:done
