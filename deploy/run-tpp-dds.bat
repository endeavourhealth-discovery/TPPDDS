@echo off
Title Discovery Data Service - TPP Data File Uploader

rem Check for uploader patch updates. If error, then end without calling uploader
java -jar bin\tpp-dds-patcher.jar <bucketname>
if %ERRORLEVEL%==-1 goto :end

rem Check for and upload data files
java -jar bin\tpp-dds-uploader.jar <mode> <rootdir> <key> <username> <password> <orgId>

:end