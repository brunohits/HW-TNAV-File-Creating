@echo off
setlocal enabledelayedexpansion

set /p path=Введите полный путь до папки Block:
set file_path=!path!
set number=%1

"C:\Program Files\RFD\tNavigator\22.4\tNav_con\tNavigator-con-v22.4.exe"	--use-gpu	gpu_mode=4	"!file_  path!\FULL_TNAV__!number!.DATA" --ecl-rsm --ecl-root -eiru --ignore-lock > log.txt 2>&1 pauseмо