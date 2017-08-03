package org.endeavourhealth.tppddsuploader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {

        AmazonS3ClientBuilder s3 = AmazonS3ClientBuilder.standard();
        String awsBucketName = args[0];
        String localDataDirectory = "c:\\discovery-ftp\\tpp\\data";
        String awsDataDirectory = "tpp/data";

        System.out.println("===========================================");
        System.out.println("     Discovery TPP Data File Uploader      ");
        System.out.println("===========================================\n");

        try {
            //Check for data files in <localDataDirectory> X and upload contents (including sub directories) to S3
            UploadDataFiles(s3.build(), localDataDirectory, awsBucketName, awsDataDirectory);
            System.exit(0);

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.exit(-1);
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
            System.exit(-1);
        } catch (InterruptedException ie) {
            System.out.println("Caught an InterruptedException: " + ie.getMessage());
            System.exit(-1);
        } catch (IOException io) {
            System.out.println("Caught an IOException: "+io.getMessage());
            System.exit(-1);
        }
    }

    //Use the AWS transfer manager to async the data files to S3
    private static void UploadDataFiles(AmazonS3 s3, String localDataDirectory, String awsBucketName, String awsDataDirectory) throws SdkBaseException, InterruptedException, IOException
    {
        TransferManagerBuilder tx = TransferManagerBuilder.standard().withS3Client(s3);
        try {
            List<File> filesToUpload = new LinkedList<File>();
            File localDataDir = new File(localDataDirectory);
            System.out.println("Checking for data upload files......\n");
            int uploadFileCount = getUploadFileList (localDataDir, filesToUpload);
            if (uploadFileCount > 0) {
                System.out.println(uploadFileCount+" data upload files found: \n");
                printUploadFiles (filesToUpload);

                MultipleFileUpload mfu = tx.build().uploadFileList(awsBucketName, awsDataDirectory, localDataDir, filesToUpload);
                showMultiUploadProgress(mfu);
                mfu.waitForCompletion();

                archiveLocalDataFiles(filesToUpload);
                System.out.println("Transfer completed at " + new Date().toString() + "\n");
            }
            else
                System.out.println("0 data upload files found.\n");
        }
        finally {
            tx.build().shutdownNow();
        }
    }

    //Get all files and sub-folders in the local Data directory for uploading, excluding the Archived folder
    private static int getUploadFileList (File localDataDir, List<File> results)
    {
        File[] filesFound = localDataDir.listFiles();
        if(filesFound != null) {
            for (File f: filesFound) {
                if(f.isDirectory()) {
                    if (!f.getName().equalsIgnoreCase("archived")) {
                        getUploadFileList(f, results);
                    }
                }
                else {
                    results.add(f);
                }
            }
            return results.size();
        }
        return 0;
    }

    //Move the uploaded files to Archive folder inline with TPP specification configuration
    private static void archiveLocalDataFiles(List<File> filesUploaded) throws IOException
    {
        for (File file: filesUploaded) {
            //Check if the batch Archived directory exists for the file, else create
            String archivedFilePath = file.getPath().replace("\\data\\","\\data\\Archived\\");
            String batchArchivedDir = archivedFilePath.substring(0, archivedFilePath.lastIndexOf("\\"));
            File batchArchivedDirectory = new File(batchArchivedDir);
            if (!Files.exists(batchArchivedDirectory.toPath()))
                Files.createDirectories(batchArchivedDirectory.toPath());

            //Move the uploaded file to the Archived folder
            File archivedFile = new File(archivedFilePath);
            Files.move(file.toPath(), archivedFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            //Check source directory is empty, then delete directory
            String batchSourceDir = file.getPath().substring(0, file.getPath().lastIndexOf("\\"));
            File batchSourceDirecory = new File(batchSourceDir);
            if (batchSourceDirecory.isDirectory() && batchSourceDirecory.list().length == 0)
                if (!batchSourceDirecory.delete())
                    throw new IOException (String.format("Failed to delete directory: %s", batchSourceDir));
        }
    }

    // Shows progress of a multiple file upload while waiting for it to finish.
    private static void showMultiUploadProgress(MultipleFileUpload mfu)
    {
        Collection<? extends Upload> subTransfers = mfu.getSubTransfers();
        List <String> doneTransfer = new ArrayList<String>();
        System.out.println("Transfer started at " + new Date().toString() + "\n");
        while (!mfu.isDone()) {
            for (Upload u : subTransfers) {
                String transferFilePath = getTransferFilePath(u.getDescription());
                TransferProgress progress = u.getProgress();
                double pct = progress.getPercentTransferred();
                if (pct >0 && !doneTransfer.contains(transferFilePath)) {
                    System.out.print(transferFilePath);
                    printProgressBar(pct);
                    System.out.println();
                }
                if (u.isDone() && !doneTransfer.contains(transferFilePath)) {
                    doneTransfer.add(transferFilePath);
                }
            }
            // wait a bit before the next update.
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                return;
            }
        }

        // The final state of the transfer. Log this?
        Transfer.TransferState finalTransferState = mfu.getState();
        System.out.println();
    }

    // Prints a simple text progressbar
    private static void printProgressBar(double pct)
    {
        final int bar_size = 40;
        final String empty_bar = "                                        ";
        final String filled_bar = "########################################";

        int amt_full = (int)(bar_size * (pct / 100.0));
        System.out.format("  [%s%s] %.2f", filled_bar.substring(0, amt_full),
                empty_bar.substring(0, bar_size - amt_full), pct);
    }

    //Print the list of files to upload to the console
    private static void printUploadFiles(List<File> filesToUpload)
    {
        StringBuilder sb = new StringBuilder();
        for (File f : filesToUpload) {
            sb.append(f.getPath()).append("\n");
        }
        System.out.println(sb.toString());
    }

    private static String getTransferFilePath(String transfer)
    {
        return transfer.substring(transfer.indexOf("/"));
    }
}
