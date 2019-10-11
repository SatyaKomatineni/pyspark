@echo off

set sparkExampleDir=C:\satya\i\spark\examples\src\main\python

if "%1" == "" goto noinput

if exist %1 goto allgood

set filename=%sparkExampleDir%\%1
goto allgood

:allgood
echo Submitting file: %filename%
c:\satya\i\spark\bin\spark-submit --master local[4] %filename% %2 %3
goto done

:noinput
echo no input file specified to submit to spark
goto done

:done
echo Command execution completed
dir %sparkExampleDir%