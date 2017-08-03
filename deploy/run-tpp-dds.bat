@echo off
Title Discovery Data Service - TPP Data File Uploader

rem Check for uploader patch updates
java -jar bin\tpp-dds-patcher.jar <bucketname>

rem Check for and upload data files
java -jar bin\tpp-dds-uploader.jar <bucketname>

:end