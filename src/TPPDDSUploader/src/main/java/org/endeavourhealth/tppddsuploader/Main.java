package org.endeavourhealth.tppddsuploader;

import org.apache.http.Header;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.endeavourhealth.tppddsuploader.HelperUtils.*;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String APPLICATION_NAME = "Discovery Data File Uploader";
    private static final String KEYCLOAK_SERVICE_URI = "https://n3auth.endeavourhealth.net/auth";
    //private static final String KEYCLOAK_SERVICE_URI = "https://devauth.endeavourhealth.net/auth";
    private static final String UPLOAD_SERVICE_URI = "https://n3messaging.endeavourhealth.net/api/PostFile?organisationId=";
    //private static final String UPLOAD_SERVICE_URI = "http://192.168.20.58:8080/api/PostFile?organisationId=";
    //private static final String UPLOAD_SERVICE_URI = "http://localhost:8083/api/PostFile?organisationId=";
    private static final int HTTP_REQUEST_TIMEOUT_MILLIS = 7200000;   //2 hours
    private static final char DEFAULT_MODE = '0';
    private static final char UI_MODE = '1';
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

        // run health checks specific to org
        clientHealthChecks(hookKey, orgId);

        try {
            List<File> inputFiles = new LinkedList<File>();
            List<File> inputFolders = new LinkedList<File>();
            switch (mode) {
                case DEFAULT_MODE:
                    System.out.println("\nChecking for data upload files......\n");
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
                default:
                    System.out.println("\nChecking for data upload files....\n");
                    inputFolders = getUploadFileList(new File(rootDir),inputFiles);
                    break;
            }

            // at least one file found or selected for uploading
            if (inputFiles.size() > 0) {
                // loop through each valid folder (batch)
                for (File inputFolder : inputFolders)
                {
                    // check validity of upload files based on orgId and batch
                    if (!checkValidUploadFiles(orgId, inputFolder))
                        continue;

                    ArrayList<Integer> intArray = new ArrayList<Integer>();
                    String folderName = inputFolder.getPath();
                    extractFileBatchLocations(inputFiles, folderName, intArray);

                    //Loop through files in folder, uploading 5 files per time, per batch folder
                    int start = intArray.get(0); int end = intArray.get(1);
                    int from = start; int to=end;
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
                        String uri = UPLOAD_SERVICE_URI.concat(orgId);
                        HttpPost httppost = new HttpPost(uri);
                        httppost.setHeader(getKeycloakToken(username, password));

                        // add each batched file into the upload
                        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
                        for (File inputFile : inputFiles.subList(from, to)) {
                            String uploadPathName = parseUploadFilePath(rootDir, inputFile);
                            entityBuilder.addBinaryBody("file", inputFile, ContentType.APPLICATION_OCTET_STREAM, uploadPathName);
                            System.out.println(inputFile + " added to transfer");
                        }
                        httppost.setEntity(entityBuilder.build());

                        // execute the upload request
                        System.out.println("\nTransfer started at " + new Date().toString() + "\n");
                        HttpResponse response = httpclient.execute(httppost);

                        int statusCode = response.getStatusLine().getStatusCode();
                        HttpEntity responseEntity = response.getEntity();
                        String responseString = EntityUtils.toString(responseEntity, "UTF-8");

                        // logout the Keycloak token session when finished
                        KeycloakClient.instance().logoutSession();

                        System.out.println("[" + statusCode + "] " + responseString);
                        postSlackAlert("Transfer status for organisationId="+orgId+" : ["+statusCode+"] "+responseString, hookKey);

                        // delete source files after successful upload of this batch
                        if (statusCode == 200) {
                            deleteSourceFiles(orgId, inputFiles.subList(from, to));
                            System.out.println("\nTransfer completed successfully at " + new Date().toString() + "\n");
                        } else {
                            System.out.println("\nTransfer failed at " + new Date().toString() + "\n");
                        }

                        from = to;
                    }while (from < end);
                }
            }
            else {
                System.out.println("0 data upload files found.\n");
                postSlackAlert("OrganisationId: "+orgId+" - 0 data upload files found.", hookKey);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Header getKeycloakToken(String username, String password) throws IOException
    {
        KeycloakClient.init(KEYCLOAK_SERVICE_URI, "endeavour", username, password, "eds-ui");
        return KeycloakClient.instance().getAuthorizationHeader();
    }
}
