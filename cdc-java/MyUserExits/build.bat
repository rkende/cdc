@echo off
REM Build script: compile all Java source files in src/ into bin/ with jars in lib/ on the classpath
setlocal
set LIBS=lib\*
if not exist bin mkdir bin
dir /b /s src\*.java > sources.txt
javac -cp "%LIBS%;." -d bin @sources.txt
del sources.txt
echo Build finished.
endlocal
