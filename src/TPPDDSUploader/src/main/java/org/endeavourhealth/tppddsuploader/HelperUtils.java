package org.endeavourhealth.tppddsuploader;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;

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

    static boolean validZipFile(File zipFile)
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
                    results.addAll(asList(f.listFiles()));
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
                    results.addAll(asList(f.listFiles()));
                    fileBatch.add(f);
                }
            }
        }

        return fileBatch;
    }

    static boolean clientHealthChecks(String orgId)
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

    static void deleteSourceFiles(String orgId, List<File> sourceFiles)
    {
        if (orgId.equalsIgnoreCase("TPP-01")) {
            for (File f : sourceFiles) {
                System.out.println("Delete file: " + f.getPath());
                f.delete();

                // check if directory is now empty and delete it
                String fileDirPath = f.getPath().substring(0, f.getPath().indexOf(f.getName()));
                File dir = new File(fileDirPath);
                if (dir.isDirectory() && dir.listFiles().length == 0)
                    dir.delete();
            }
        }
    }

    static boolean checkValidUploadFiles(String orgId, File fileFolder)
    {
        // TPP client application as source - check number of files and structure of main zip
        if (orgId.equalsIgnoreCase("TPP-01"))
        {
            File [] folderFiles = fileFolder.listFiles();
            int fileCount = folderFiles.length;
            if (fileCount != 4)
            {
                System.out.println(String.format("Invalid number of files (%d) detected in folder: %s",fileCount,fileFolder.getPath()));
                return false;
            }
            else {
                List<String> fileCheckArray = new ArrayList<String>(Arrays.asList("SRExtract.zip","SRManifest.csv","SRMapping.csv","SRMappingGroup.csv"));
                for (File df : folderFiles)
                {
                    boolean validFile = fileCheckArray.contains(df.getName());
                    if (!validFile)
                    {
                        System.out.println(String.format("Invalid file (%s) detected in folder: %s",df.getName(), fileFolder.getPath()));
                        return false;
                    }

                    // validate zip file
                    if (df.getName().equalsIgnoreCase("SRExtract.zip"))
                    {
                        if (!validZipFile(df))
                        {
                            System.out.println(String.format("Invalid Zip file (%s) detected in folder: %s ",df.getName(),fileFolder.getPath()));
                            return false;
                        }
                    }
                }
            }
            return true;
        }
        else
        if (orgId.equalsIgnoreCase("HOM-01"))
        {
            return true;
        }

        return true;
    }
}
