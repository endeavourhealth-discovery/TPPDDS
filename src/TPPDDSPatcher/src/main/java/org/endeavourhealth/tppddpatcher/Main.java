package org.endeavourhealth.tppddpatcher;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {

        AmazonS3ClientBuilder s3 = AmazonS3ClientBuilder.standard();
        String bucketName = "discovery-tpp-inbound";
        String localWorkingDirectory = "c:\\TPPDDS";
        String awsPatchDirectory = "patch";

        System.out.println("===========================================");
        System.out.println("          Discovery TPP Patcher            ");
        System.out.println("===========================================\n");

        try {
            CheckForPatchUpdates(s3.build(),awsPatchDirectory, bucketName, localWorkingDirectory);
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
            System.out.println("Caught an InterruptedException: "+ie.getMessage());
            System.exit(-1);
        } catch (IOException io) {
            System.out.println("Caught an IOException: "+io.getMessage());
            System.exit(-1);
    }
    }

    private static void CheckForPatchUpdates(AmazonS3 s3, String awsPatchDirectory, String bucketName, String localWorkingDirectory) throws SdkBaseException, InterruptedException, IOException
    {
        System.out.println("Checking for patch updates......\n");

        ListObjectsRequest lor = new ListObjectsRequest();
        lor.setBucketName(bucketName);
        lor.setPrefix(awsPatchDirectory);
        ObjectListing ol = s3.listObjects(lor);
        List<S3ObjectSummary> patchFiles = ol.getObjectSummaries();    //returns directory name and any files, so 1+ results = patch available
        if (patchFiles.size()>1)
        {
            System.out.println("Patch update available, downloading...... \n");
            TransferManagerBuilder tx = TransferManagerBuilder.standard().withS3Client(s3);
            try {
                MultipleFileDownload mfd = tx.build().downloadDirectory(bucketName, awsPatchDirectory, new File(localWorkingDirectory));
                mfd.waitForCompletion();

                PatchLocalFile(localWorkingDirectory);
            }
            finally {
                tx.build().shutdownNow();
            }

            //TODO:If successful, delete patch files?
        }
        else {
            System.out.println("No patch update available.\n");
        }
    }

    private static void PatchLocalFile(String localWorkingDirectory) throws IOException {

        String patchFilename = localWorkingDirectory+"\\patch\\tpp-dds-uploader-patch.jar";
        File patchFile = new File(patchFilename);
        if (Files.exists(patchFile.toPath()))
        {
            String existingFilename = localWorkingDirectory + "\\bin\\tpp-dds-uploader.jar";
            File existingFile = new File(existingFilename);
            Files.copy(patchFile.toPath(), existingFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        else
        {
            throw new IOException (String.format("Patch file %s does not exist", patchFilename));
        }
    }
}
