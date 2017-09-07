package org.endeavourhealth.tppddsuploader;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.*;

import static java.util.Arrays.asList;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String APPLICATION_NAME = "Discovery Data File Uploader";
    private static final String KEYCLOAK_SERVICE_URI = "https://devauth.endeavourhealth.net/auth";
    private static final String UPLOAD_SERVICE_URI = "http://localhost:8083/api/PostFile?organisationId=";
    private static final int HTTP_REQUEST_TIMEOUT_MILLIS = 7200000;   //2 hours
    private static final char DEFAULT_MODE = '0';
    private static final char UI_MODE = '1';

    public static void main(String[] args) throws IOException {

        char mode = DEFAULT_MODE;
        String key = ""; String username = ""; String password = ""; String rootDir = ""; String orgId = "";
        if (args.length > 0) mode = args[0].toCharArray()[0];
        if (args.length > 1) rootDir = args[1];
        if (args.length > 2) key = args[2];
        if (args.length > 3) username = args[3];
        if (args.length > 4) password = args[4];
        if (args.length > 5) orgId = args[5];
        DataFileUpload(mode, rootDir, key, username, password, orgId);
    }

    private static void DataFileUpload(char mode, String rootDir, String key, String username, String password, String orgId)
    {
        System.out.println("===========================================");
        System.out.println("   "+APPLICATION_NAME+" (".concat(orgId)+")");
        System.out.println("===========================================\n");

        // run health checks specific to org
        clientHealthChecks(orgId);

        try {
            List<File> inputFiles = new LinkedList<File>();
            switch (mode) {
                case DEFAULT_MODE:
                    System.out.println("\nChecking for data upload files......\n");
                    getUploadFileList(new File(rootDir),inputFiles);
                    break;
                case UI_MODE:
                    JFileChooser fileChooser = new JFileChooser();
                    fileChooser.setDialogTitle(APPLICATION_NAME + " - Choose the data files to upload...");
                    fileChooser.setCurrentDirectory(new File(System.getProperty("user.home")));
                    fileChooser.setMultiSelectionEnabled(true);
                    int result = fileChooser.showOpenDialog(null);
                    if (result == JFileChooser.APPROVE_OPTION) {
                        inputFiles = asList(fileChooser.getSelectedFiles());
                    } else
                        System.exit(0);
                    break;
                default:
                    System.out.println("\nChecking for data upload files....\n");
                    getUploadFileList(new File(rootDir),inputFiles);
                    break;
            }

            // at least one file found or selected
            if (inputFiles.size() > 0) {

                // check validity of upload files based on orgId
                if (!checkValidUploadFiles(orgId, inputFiles))
                    System.exit(-1);

                // set service timeout limits
                RequestConfig requestConfig = RequestConfig
                        .custom()
                        .setConnectTimeout(HTTP_REQUEST_TIMEOUT_MILLIS)
                        .setSocketTimeout(HTTP_REQUEST_TIMEOUT_MILLIS)
                        .setConnectionRequestTimeout(HTTP_REQUEST_TIMEOUT_MILLIS)
                        .build();

                CloseableHttpClient httpclient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

                // create the upload http service URL with Keycloak authorisation header
                String uri = UPLOAD_SERVICE_URI.concat(orgId);
                HttpPost httppost = new HttpPost(uri);
                httppost.setHeader(getKeycloakToken(key, username, password));

                // add each file into the upload
                MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
                for (File inputFile : inputFiles) {
                    String uploadPathName = parseUploadFilePath(rootDir, inputFile);
                    entityBuilder.addBinaryBody("file", inputFile, ContentType.APPLICATION_OCTET_STREAM, uploadPathName);
                    System.out.println(inputFile+" added to transfer");
                }
                httppost.setEntity(entityBuilder.build());

                // execute the upload request
                System.out.println("\nTransfer started at " + new Date().toString() + "\n");
                HttpResponse response = httpclient.execute(httppost);

                int statusCode = response.getStatusLine().getStatusCode();
                HttpEntity responseEntity = response.getEntity();
                String responseString = EntityUtils.toString(responseEntity, "UTF-8");

                // delete source files after successful upload
                if (statusCode == 200)
                {
                    deleteSourceFiles(inputFiles);
                }

                System.out.println("[" + statusCode + "] " + responseString);
                System.out.println("\nTransfer completed at " + new Date().toString() + "\n");
            }
            else
                System.out.println("0 data upload files found.\n");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Header getKeycloakToken(String key, String username, String password) throws IOException
    {
        KeycloakClient.init(KEYCLOAK_SERVICE_URI, "endeavour", username, password, "eds-ui");
        return KeycloakClient.instance().getAuthorizationHeader();
    }

    //Get all files and sub-folders in the local Data directory for uploading, excluding the Archived folder
    private static int getUploadFileList (File localDataDir, List<File> results)
    {
        File[] filesFound = localDataDir.listFiles();
        if(filesFound != null) {
            for (File f: filesFound) {
                if(f.isDirectory()) {
                    getUploadFileList(f, results);
                }
                else {
                    results.add(f);
                }
            }
            return results.size();
        }
        return 0;
    }

    private static String parseUploadFilePath(String rootDir, File filePath)
    {
        // remove Archived path level and root directory from the file path.  We only want the baseline folder and filename
        String tempFilePath = filePath.getPath().replace("Archived\\","");
        return tempFilePath.substring(rootDir.length());
    }

    private static String parseDirFilePath(File filePath)
    {
        return filePath.getPath().substring(0,filePath.getPath().lastIndexOf("\\"));
    }

    private static void deleteSourceFiles(List<File> sourceFiles)
    {
        for (File f: sourceFiles)
        {
            System.out.println("Delete file: "+f.getPath());
            f.delete();

            // check if directory is now empty and delete it
            String fileDirPath = f.getPath().substring(0,f.getPath().indexOf(f.getName()));
            File dir = new File(fileDirPath);
            if (dir.isDirectory() && dir.listFiles().length == 0)
                dir.delete();
        }
    }

    private static boolean clientHealthChecks(String orgId)
    {
        if (orgId.isEmpty())
            return true;

        // TPP client application as source - check TCP ports to denote application running
        if (orgId.equalsIgnoreCase("TPP-01"))
        {
            System.out.println("Performing TPP client checks......\n");
            try
            {
                (new Socket("localhost", 40700)).close();
                return true;
            }
            catch (IOException ex1)
            {
                System.out.println("Unable to connect to port 40700. Trying port 2135...");
                //TODO: log this alert
                try
                {
                    (new Socket("localhost", 2135)).close();
                    return true;
                }
                catch (IOException ex2)
                {
                    //TODO: log this alert
                    System.out.println("Unable to connect to port 2135.  TPP client application not running.");
                    return false;
                }
            }
        }
        else
        if (orgId.equalsIgnoreCase("HOM-01")) {
            return false;
        }

        return false;
    }

    private static boolean checkValidUploadFiles(String orgId, List<File> inputFiles)
    {
        // TPP client application as source - check number of files and structure of main zip
        if (orgId.equalsIgnoreCase("TPP-01"))
        {
            // process input files batch for checking
            List<String> foldersChecked = new ArrayList<String>();
            for (File f : inputFiles)
            {
                // we are only interested in each batch folder
                String folder = parseDirFilePath(f);
                if (foldersChecked.contains(folder))
                    continue;

                File fileFolder = new File(folder);
                File [] folderFiles = fileFolder.listFiles();
                int fileCount = folderFiles.length;
                if (fileCount != 4)
                {
                    System.out.println(String.format("Invalid number of files (%d) detected in folder: %s",fileCount,folder));
                    return false;
                }
                else {
                    List<String> fileCheckArray = new ArrayList<String>(Arrays.asList("SRExtract.zip","SRManifest.csv","SRMapping.csv","SRMappingGroup.csv"));
                    for (File df : folderFiles)
                    {
                        boolean validFile = fileCheckArray.contains(df.getName());
                        if (!validFile)
                        {
                            System.out.println(String.format("Invalid file (%s) detected in folder: %s",df.getName(), folder));
                            return false;
                        }

                        // validate zip file
                        if (df.getName().equalsIgnoreCase("SRExtract.zip"))
                        {
                            if (!validZipFile(df))
                            {
                                System.out.println(String.format("Invalid Zip file (%s) detected in folder: %s ",df.getName(),folder));
                                return false;
                            }
                        }
                    }
                }
                foldersChecked.add(folder);
            }

            System.out.println(String.format("%d valid data upload files found in %d folders: \n", inputFiles.size(), foldersChecked.size()));
            return true;
        }
        else
        if (orgId.equalsIgnoreCase("HOM-01"))
        {
            return true;
        }

        return true;
    }

    private static boolean validZipFile(File zipFile)
    {
        try
        {
            ZipFile zip = new ZipFile(zipFile.getPath());

            // is the zip file a valid zip?
            if (!zip.isValidZipFile())
                return false;

            // loop through the file headers and check for rogue files
            List fileHeaderList = zip.getFileHeaders();
            for (int i = 0; i < fileHeaderList.size(); i++)
            {
                FileHeader fileHeader = (FileHeader) fileHeaderList.get(i);
                String fileName = fileHeader.getFileName();
                // is the file a .csv?
                if (!fileName.contains(".csv")) {
                    System.out.println(String.format("Invalid file (%s) detected in Zip file: %s",fileName, zipFile.getPath()));
                    return false;
                }
            }

        } catch (ZipException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
}
