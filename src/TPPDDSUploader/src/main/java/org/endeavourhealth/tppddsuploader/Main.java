package org.endeavourhealth.tppddsuploader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;

import static java.util.Arrays.asList;
import static org.endeavourhealth.tppddsuploader.HelperUtils.*;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String APPLICATION_NAME = "Discovery Data File Uploader";
    private static final String KEYCLOAK_SERVICE_URI = "https://auth.discoverydataservice.net/auth";
    private static final String UPLOAD_SERVICE_URI = "https://n3messageapi.discoverydataservice.net/api/PostFile?organisationId=";
    private static final int HTTP_REQUEST_TIMEOUT_MILLIS = 7200000;   //2 hours
    private static final char DEFAULT_MODE = '0';
    private static final char UI_MODE = '1';
    private static final char TEST_MODE = '2';
    private static final int MAX_FILE_BATCH = 5;

    public static void main(String[] args) throws IOException {

        char mode = DEFAULT_MODE;
        String hookKey = ""; String username = ""; String password = ""; String rootDir = ""; String orgId = "";
        if (args.length > 0) mode = args[0].toCharArray()[0];
        if (args.length > 1) rootDir = args[1];
        if (args.length > 2) hookKey = args[2];
        if (args.length > 3) username = args[3];
        if (args.length > 4) password = args[4];
        if (args.length > 5) orgId = args[5];
        DataFileUpload(mode, rootDir, hookKey, username, password, orgId);
    }

    private static void DataFileUpload(char mode, String rootDir, String hookKey, String username, String password, String orgId)
    {
        System.out.println("===========================================");
        System.out.println("   "+APPLICATION_NAME+" (".concat(orgId)+")");
        System.out.println("===========================================\n");

        try {
            List<File> inputFiles = new LinkedList<File>();
            List<File> inputFolders = new LinkedList<File>();
            switch (mode) {
                case DEFAULT_MODE:
                    // run health checks specific to org
                    clientHealthChecks(hookKey, orgId);
                    // check root director for presence of files and report name and size
                    monitorRootDirectoryFiles (new File(rootDir), orgId, hookKey);

                    System.out.println("\nChecking for data upload files......\n");
                    postSlackAlert("OrganisationId: "+orgId+" - Checking for data upload files......", hookKey, null);
                    inputFolders = getUploadFileList(new File(rootDir),inputFiles);
                    break;
                case UI_MODE:
                    JFileChooser fileChooser = new JFileChooser();
                    fileChooser.setDialogTitle(APPLICATION_NAME + " - Choose the data files to upload...");
                    fileChooser.setCurrentDirectory(new File(rootDir));

                    fileChooser.setMultiSelectionEnabled(true);
                    int result = fileChooser.showOpenDialog(null);
                    if (result == JFileChooser.APPROVE_OPTION) {
                        inputFiles = asList(fileChooser.getSelectedFiles());
                        rootDir = fileChooser.getCurrentDirectory().getPath()+"\\";
                        inputFolders.add(new File(rootDir));
                        //TODO: process selected files and zip to multi-zip if large
                    } else
                        System.exit(0);
                    break;
                case TEST_MODE:
                    runTestMode (KEYCLOAK_SERVICE_URI, hookKey, username, password, orgId);
                    System.exit(0);
                    break;
                default:
                    System.out.println("\nChecking for data upload files......\n");
                    inputFolders = getUploadFileList(new File(rootDir),inputFiles);
                    break;
            }

            // at least one file found or selected for uploading
            if (inputFiles.size() > 0) {

                int validFiles = 0;

                // loop through each valid folder (batch)
                for (File inputFolder : inputFolders)
                {
                    // check validity of upload files based on orgId and batch
                    if (!checkValidUploadFiles(orgId, inputFolder, hookKey))
                        continue;

                    validFiles++;

                    ArrayList<Integer> intArray = new ArrayList<Integer>();
                    String folderName = inputFolder.getPath();
                    extractFileBatchLocations(inputFiles, folderName, intArray);

                    int start = intArray.get(0); int end = intArray.get(1);
                    int from = start; int to = end; int fileCount = to - from;
                    System.out.println("\n" + fileCount + " valid data upload files found in " + inputFolder + "\n");
                    postSlackAlert("OrganisationId: "+orgId+" - "+fileCount + " valid data upload files found in " + inputFolder, hookKey, null);

                    //Loop through files in folder, uploading 5 files per time, per batch folder
                    do
                    {
                        to = from + MAX_FILE_BATCH;
                        if (to > end) to = end;

                        // set service timeout limits
                        RequestConfig requestConfig = RequestConfig
                                .custom()
                                .setConnectTimeout(HTTP_REQUEST_TIMEOUT_MILLIS)
                                .setSocketTimeout(HTTP_REQUEST_TIMEOUT_MILLIS)
                                .setConnectionRequestTimeout(HTTP_REQUEST_TIMEOUT_MILLIS)
                                .build();

                        HttpRequestRetryHandler retryHandler = new HttpRequestRetryHandler() {
                            public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
                                if (exception instanceof InterruptedIOException || exception instanceof UnknownHostException)
                                    return false;
                                if (exception instanceof ConnectException || exception instanceof SSLException)
                                    return false;
                                return !(exception instanceof SocketException);
                            }
                        };

                        CloseableHttpClient httpclient = HttpClientBuilder
                                .create()
                                .setRetryHandler(retryHandler)
                                .setDefaultRequestConfig(requestConfig).build();

                        // create the upload http service URL with Keycloak authorisation header
                        System.out.println("Authenticating.......\n");
                        String uri = UPLOAD_SERVICE_URI.concat(orgId);
                        HttpPost httppost = new HttpPost(uri);
                        httppost.setHeader(getKeycloakToken(KEYCLOAK_SERVICE_URI, username, password));

                        postSlackAlert("OrganisationId: "+orgId+" - Authenticated OK", hookKey, null);

                        // add each batched file into the upload
                        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
                        for (File inputFile : inputFiles.subList(from, to)) {
                            String uploadPathName = parseUploadFilePath(rootDir, inputFile);
                            entityBuilder.addBinaryBody("file", inputFile, ContentType.APPLICATION_OCTET_STREAM, uploadPathName);
                            System.out.println(inputFile + " added to transfer");
                        }

                        postSlackAlert("OrganisationId: "+orgId+" - Adding files to transfer", hookKey, null);

                        httppost.setEntity(entityBuilder.build());

                        // execute the upload request
                        System.out.println("\nTransfer started at " + new Date().toString() + "\n");
                        postSlackAlert("OrganisationId: "+orgId+" - Transfer started at " + new Date().toString(), hookKey, null);

                        HttpResponse response = httpclient.execute(httppost);

                        int statusCode = response.getStatusLine().getStatusCode();
                        HttpEntity responseEntity = response.getEntity();
                        String responseString = EntityUtils.toString(responseEntity, "UTF-8");

                        // logout the Keycloak token session when finished
                        KeycloakClient.instance().logoutSession();

                        System.out.println("[" + statusCode + "] " + responseString);

                        // delete source files after successful upload of this batch
                        String fileDetails = "";
                        if (statusCode == 200) {
                            deleteSourceFiles(orgId, inputFiles.subList(from, to));
                            System.out.println("\nTransfer completed successfully at " + new Date().toString() + "\n");

                            fileDetails = " - " + inputFiles.subList(from, to).toString();
                        } else {
                            System.out.println("\nTransfer failed at " + new Date().toString() + "\n");
                        }

                        postSlackAlert("Transfer status for organisationId="+orgId+" : ["+statusCode+"] "+responseString + fileDetails, hookKey, null);

                        from = to;
                    }while (from < end);
                }

                //if none of the input files are valid, set alert
                if (validFiles == 0) {
                    postSlackAlert("OrganisationId: "+orgId+" - 0 valid data upload files found.", hookKey, null);
                }
            }
            else {
                System.out.println("0 data upload files found in " + rootDir + "\n");
                postSlackAlert("OrganisationId: "+orgId+" - 0 data upload files found.", hookKey, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            postSlackAlert("Exception occured during upload for OrganisationId: "+orgId, hookKey, e.getMessage());
        }
    }
}
