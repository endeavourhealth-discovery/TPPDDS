package org.endeavourhealth.tppddsuploader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {

        AmazonS3ClientBuilder s3 = AmazonS3ClientBuilder.standard();
        String bucketName = "discovery-tpp-inbound";
        String localDataDirectory = "c:\\temp\\data";
        String awsDataDirectory = "data";

        System.out.println("===========================================");
        System.out.println("       Discovery TPP Data Uploader         ");
        System.out.println("===========================================\n");

        try {
            //Check for data files in <localDataDirectory> X and upload contents (including sub directories) to S3
            UploadDataFiles(s3.build(), localDataDirectory, bucketName, awsDataDirectory);
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
        }
    }

    private static void UploadDataFiles(AmazonS3 s3, String localDataDirectory, String bucketName, String awsDataDirectory) throws SdkBaseException, InterruptedException
    {
        System.out.println("Checking for data upload files......\n");

        TransferManagerBuilder tx = TransferManagerBuilder.standard().withS3Client(s3);
        try {
            MultipleFileUpload mfu = tx.build().uploadDirectory(bucketName, awsDataDirectory, new File(localDataDirectory), true);

            showMultiUploadProgress(mfu);
            mfu.waitForCompletion();

            System.out.println("\nTransfer completed at " + new Date().toString() + "\n");

            //TODO: If successful, delete/move source files?
        }
        finally {
            tx.build().shutdownNow();
        }
    }

    // Prints progress of a multiple file upload while waiting for it to finish.
    // Simple output to the console or you could log progress elsewhere
    private static void showMultiUploadProgress(MultipleFileUpload mfu)
    {
        Collection<? extends Upload> subTransfers = mfu.getSubTransfers();
        List <String> doneTransfer = new ArrayList<String>();

        printUploadFiles(subTransfers);

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
    }

    // prints a simple text progressbar: [#####     ]
    private static void printProgressBar(double pct)
    {
        // if bar_size changes, then change erase_bar (in eraseProgressBar) to
        // match.
        final int bar_size = 40;
        final String empty_bar = "                                        ";
        final String filled_bar = "########################################";

        int amt_full = (int)(bar_size * (pct / 100.0));
        System.out.format("  [%s%s] %.2f", filled_bar.substring(0, amt_full),
                empty_bar.substring(0, bar_size - amt_full), pct);
    }

    private static void printUploadFiles(Collection<? extends Upload> subTransfers)
    {
        StringBuilder sb = new StringBuilder();
        for (Upload u : subTransfers) {
            String uploadDescription = u.getDescription();
            sb.append(getTransferFilePath(uploadDescription)).append("\n");
        }
        System.out.println(sb.toString());
    }

    private static String getTransferFilePath(String transfer)
    {
        return transfer.substring(transfer.indexOf("/"));
    }
}
