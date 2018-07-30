package org.endeavourhealth.tppddsuploader;

import com.google.common.base.Strings;
import net.gpedro.integrations.slack.SlackApi;
import net.gpedro.integrations.slack.SlackAttachment;
import net.gpedro.integrations.slack.SlackException;
import net.gpedro.integrations.slack.SlackMessage;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.apache.http.Header;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;

class HelperUtils {

    private static final long ZIP_SPLIT_SIZE = 10485760;
    private static final String TPP_ORGS = "TPP-01,F86638";

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

    static List<File> splitLargeZipFiles(List<File> inputFiles) {
        List<File> outputFiles = new LinkedList<File>();
        for (File f : inputFiles) {
            if (!f.isDirectory()) {
                if (validZipFile(f)) {
                    try {
                        long fileSize = f.length();
                        if (fileSize > ZIP_SPLIT_SIZE) {
                            System.out.println("Large zip file found: " + f.getPath() + " (" + fileSize + " bytes). Extracting....");
                            try {
                                // extract existing large zip
                                ZipFile inZipFile = new ZipFile(f.getPath());
                                String fileName = f.getName();
                                String zipFolder = f.getPath().substring(0, f.getPath().indexOf(fileName)) + "temp";
                                inZipFile.extractAll(zipFolder);

                                // delete large zip file
                                f.delete();

                                // re-zip, this time splitting into multi part
                                System.out.println("Re-zipping files over multiple split parts....");
                                ZipFile outZipFile = new ZipFile(f.getPath());
                                ZipParameters parameters = new ZipParameters();
                                parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
                                parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
                                parameters.setIncludeRootFolder(false);
                                outZipFile.createZipFileFromFolder(zipFolder, parameters, true, ZIP_SPLIT_SIZE);

                                // get the array of new zip parts, convert to File and add to output
                                ArrayList<String> zipFileParts = outZipFile.getSplitZipFiles();
                                for (String fileStr: zipFileParts) {
                                    if (fileStr.contains(".z010"))
                                        fileStr = fileStr.replace(".z010",".z10");  // source lib bug
                                    outputFiles.add(new File(fileStr));
                                }

                                // remove source files
                                DeleteDirectory(zipFolder);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        else{
                            outputFiles.add(f);
                        }
                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                    }
                }
                else{
                    outputFiles.add(f);
                }
            }
        }
        return outputFiles;
    }

    static void DeleteDirectory(String directory)
    {
        File[] files = new File(directory).listFiles();
        for (File f: files) {
            if (f.isDirectory())
                DeleteDirectory(f.getPath());
            else
                f.delete();
        }

        new File(directory).delete();
    }


    static void extractFileBatchLocations(List<File> inputFiles , String folderName, ArrayList<Integer> intArray)
    {
        int index = -1; int start = -1; int end = -1;
        for (File f : inputFiles)
        {
            index++;
            if (f.getPath().substring(0,folderName.length()).equalsIgnoreCase(folderName)) {
                if (start == -1) {
                    start = index;
                    end = start;
                }
                end++;
            }
        }
        intArray.add(start); intArray.add(end);
    }

    static void monitorRootDirectoryFiles (File localDataRootDir, String orgId, String hookKey) {

        File[] filesFound = localDataRootDir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return !pathname.isDirectory();
            }
        });
        if(filesFound != null && filesFound.length>0) {
            System.out.println(String.format("\nFiles detected root directory: %s", localDataRootDir.getPath()));
            postSlackAlert("OrganisationId: "+orgId+" - "+ String.format("Files detected in root directory: %s", localDataRootDir.getPath()), hookKey, fileListDisplay(filesFound));
        }
    }

    static String parseUploadFilePath(String rootDir, File filePath)
    {
        // remove Archived path level and root directory from the file path.  We only want the baseline folder and filename
        String tempFilePath = filePath.getPath().replace("Archived\\","");
        return tempFilePath.substring(rootDir.length());
    }

    //Get all files and sub-folders in the local Data directory for uploading, including the Archived folder
    static List<File> getUploadFileList (File localDataDir, List<File> results)
    {
        List<File> fileBatch = new LinkedList<File>();

        // check for archived file batches first
        File archivedFolder = new File(localDataDir.getPath().concat("\\Archived"));
        File[] archivedFoldersFound = archivedFolder.listFiles();
        if(archivedFoldersFound != null) {
            for (File f : archivedFoldersFound) {
                if (f.isDirectory() && f.listFiles()!= null) {
                    //splitLargeZipFiles(asList(f.listFiles()));

                    results.addAll(splitLargeZipFiles(asList(f.listFiles())));
                    fileBatch.add(f);
                }
            }
        }

        // now get non Archived files.  Should only be a single folder
        File[] filesFound = localDataDir.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return !pathname.getName().equalsIgnoreCase("Archived") && pathname.isDirectory();
            }
        });
        if(filesFound != null) {
            for (File f: filesFound) {
                if(f.isDirectory() && f.listFiles()!= null) {
                    results.addAll(splitLargeZipFiles(asList(f.listFiles())));
                    fileBatch.add(f);
                }
            }
        }

        return fileBatch;
    }

    static boolean clientHealthChecks(String hookKey, String orgId)
    {
        if (orgId.isEmpty())
            return true;

        // TPP client application as source - check TCP ports to denote application running
        if (TPP_ORGS.contains(orgId))
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
                postSlackAlert("OrganisationId: "+orgId+" - Unable to connect to port 40700. Trying port 2135...", hookKey, null);
                try
                {
                    (new Socket("localhost", 2135)).close();
                    return true;
                }
                catch (IOException ex2)
                {
                    System.out.println("Unable to connect to port 2135.  TPP client application not running.");
                    postSlackAlert("OrganisationId: "+orgId+" - Unable to connect to port 2135.  TPP client application not running.", hookKey, null);
                    return false;
                }
            }
        }

        return false;
    }

    static void deleteSourceFiles(String orgId, List<File> sourceFiles)
    {
        if (TPP_ORGS.contains(orgId)) {
            for (File f : sourceFiles) {
                System.out.println("Delete local file: " + f.getPath());
                f.delete();

                // check if directory is now empty and delete it
                String fileDirPath = f.getPath().substring(0, f.getPath().indexOf(f.getName()));
                File dir = new File(fileDirPath);
                if (dir.isDirectory() && dir.listFiles().length == 0)
                    dir.delete();
            }
        }
    }

    static String fileListDisplay (File [] files) {
        String display = "";

        for (File f : files) {
            long sizeKb = f.length() / 1024;
            display = display.concat(f.getName() + " : " + Long.toString(sizeKb) +  " kb\n");
        }

        return display;
    }

    static boolean checkValidUploadFiles(String orgId, File fileFolder, String hookKey)
    {
        // TPP client application as source - check number of files and structure of main zip
        if (TPP_ORGS.contains(orgId))
        {
            int fileCount = countFiles (fileFolder, false);
            File [] folderFiles = fileFolder.listFiles();
            if (fileCount != 4)
            {
                System.out.println(String.format("Invalid number of files (%d) detected in folder: %s",fileCount,fileFolder.getPath()));

                postSlackAlert("OrganisationId: "+orgId+" - "+ String.format("Invalid number of files (%d) detected in folder: %s",fileCount,fileFolder.getPath()), hookKey, fileListDisplay(folderFiles));
                return false;
            }
            else {
                List<String> fileCheckArray = new ArrayList<String>(Arrays.asList("SRExtract.zip","SRManifest.csv","SRMapping.csv","SRMappingGroup.csv"));
                //File [] folderFiles = fileFolder.listFiles();
                for (File df : folderFiles)
                {
                    String fileExt = df.getName().substring(df.getName().lastIndexOf("."));
                    boolean validFile = (fileCheckArray.contains(df.getName()) || fileCheckArray.contains(df.getName().replace(fileExt,".zip")));
                    if (!validFile)
                    {
                        System.out.println(String.format("Invalid file (%s) detected in folder: %s",df.getName(), fileFolder.getPath()));
                        postSlackAlert("OrganisationId: "+orgId+" - "+ String.format("Invalid file (%s) detected in folder: %s",df.getName(), fileFolder.getPath()), hookKey, fileListDisplay(folderFiles));
                        return false;
                    }

                    // validate zip file
                    if (df.getName().equalsIgnoreCase("SRExtract.zip"))
                    {
                        if (!validZipFile(df))
                        {
                            System.out.println(String.format("Invalid Zip file (%s) detected in folder: %s ",df.getName(),fileFolder.getPath()));
                            postSlackAlert("OrganisationId: "+orgId+" - "+ String.format("Invalid Zip file (%s) detected in folder: %s ",df.getName(),fileFolder.getPath()), hookKey, null);
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        return true;
    }

    static int countFiles(File folder, boolean includeMulti)
    {
        File [] folderFiles = folder.listFiles();
        List<String> fileList = new ArrayList<String>();
        for (File f: folderFiles)
        {
            String filePrefix = f.getName().substring(0,f.getName().lastIndexOf("."));
            if (includeMulti || !fileList.contains(filePrefix))
                fileList.add(filePrefix);
        }

        return fileList.size();
    }

    static void postSlackAlert(String message, String hookKey, String exceptionMessage)
    {
        SlackMessage slackMessage = new SlackMessage(message);

        if (!Strings.isNullOrEmpty(exceptionMessage)) {
            SlackAttachment slackAttachment = new SlackAttachment();
            slackAttachment.setFallback("Exception cannot be displayed");
            slackAttachment.setText("```" + exceptionMessage + "```");
            slackAttachment.addMarkdownAttribute("text");
            slackMessage.addAttachments(slackAttachment);
        }

        String url = "https://hooks.slack.com/services/T3MF59JFJ/B7DFYMUJK/";
        url = url.concat(hookKey);
        try {
            //SocketAddress sa = InetSocketAddress.createUnresolved("nat.endeavourhealth.net", 3124);
            //Proxy proxy = new Proxy(Proxy.Type.HTTP,sa);
            //SlackApi slackApi = new SlackApi(url, 10000, proxy);
            SlackApi slackApi = new SlackApi(url);
            slackApi.call(slackMessage);
        }
        catch (SlackException ex) {
            System.out.println ("\nSlack integration failure. Unable to create alert => "+ex.getMessage());
        }
    }

    static Header getKeycloakToken(String keycloakURI, String username, String password) throws IOException
    {
        KeycloakClient.init(keycloakURI, "endeavour-machine", username, password, "dds-api");
        return KeycloakClient.instance().getAuthorizationHeader();
    }

    static void runTestMode (String keycloakURI, String hookKey, String username, String password, String orgId) {
        System.out.println("\nEntering test mode......\n");
        System.out.println("\n(1) Authentication test....");
        try {
            String token = getKeycloakToken(keycloakURI, username, password).getValue();
            System.out.println("\nToken => "+token+"\n");
        } catch (Exception ex) {
            System.out.println("\nAuthentication failed => "+ex.getMessage());
        }

        System.out.println("\n(2) Slack alerts test....\n");
        postSlackAlert("Test alert for OrganisationId = "+orgId, hookKey, null);
    }
}
