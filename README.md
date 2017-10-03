### Discovery Data Service - data uploader for client PCs

##### [install directory]
`Typically c:\discovery-ftp\bin` 

##### [location of downloaded patch files]
`[install directory]\bin\patch`

##### [batch file that calls the two executable jars]
`\run-tpp-dds.bat`

##### [patcher application - checks for and downloads patch files]
`\bin\tpp-dds-patcher.jar [bucketname] [localdir]`

`[bucketname] - Amazon S3 bucket for patch files`

`[localdir] - Local install directory for uploader app`

##### [data file uploader application - checks for and uploads data files]
`\bin\tpp-dds-uploader.jar [mode] [rootdir] [hookkey] [username] [password] [orgId]`

`[mode] - 0 = Auto file detect in [rootdir], no UI. 1 = File chooser UI mode, opens in [rootdir]`

`[rootdir] - Data file extract folder (mode 0) or File chooser starting folder (mode 1)`  

`[hookkey] - Slack alert integration web hook key`

`[username] - Keycloak username`

`[password] - Keycloak password`

`[orgId] - Data publishing Organisation Id`

##### [location of data files for upload]
`Typically c:\extract`

##### [location of archived uploaded data files]
`[data file upload location]\archived`
