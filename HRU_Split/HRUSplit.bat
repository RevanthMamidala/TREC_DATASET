@echo off
REM === Set project and output names ===
set PROJECT_DIR=D:\TREC Models\Minnesota Models\Minnesota_CDL_20250625
set OUTPUT_NAME=HRU2_Split_Recursive

REM === SWAT+ output file and variable to add ===
set TXT_FILE=%PROJECT_DIR%\Scenarios\Default\TxtInOut\hru_wb_aa.txt
set VARIABLE=qtile
set FINAL_OUTPUT=%OUTPUT_NAME%_qtile.shp

REM === Script paths ===
set SCRIPT_DIR=%~dp0
set SPLIT_SCRIPT=%SCRIPT_DIR%SplitHRU_Shape.py
set ADDCOL_SCRIPT=%SCRIPT_DIR%Add_datafrom_outputs_to_shp.py

echo.
echo Step 1: Running HRU Split script...
python "%SPLIT_SCRIPT%" "%PROJECT_DIR%" "%OUTPUT_NAME%"
if errorlevel 1 (
    echo HRU Split script failed.
    pause
    exit /b
)

echo.
echo Step 2: Adding variable '%VARIABLE%' from SWAT+ file...
python "%ADDCOL_SCRIPT%" "%TXT_FILE%" "%VARIABLE%" "%FINAL_OUTPUT%" "%PROJECT_DIR%"
if errorlevel 1 (
    echo Adding column failed.
    pause
    exit /b
)

echo.
echo    All done! Final shapefile saved to:
echo    %PROJECT_DIR%\Outputs\%FINAL_OUTPUT%
pause

