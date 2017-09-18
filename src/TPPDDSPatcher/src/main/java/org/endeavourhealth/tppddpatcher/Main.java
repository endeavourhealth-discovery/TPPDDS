package org.endeavourhealth.tppddpatcher;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String APPLICATION_NAME = "Discovery Data File Uploader Patcher";

    public static void main(String[] args) throws IOException {
        AmazonS3ClientBuilder s3 = AmazonS3ClientBuilder.standard();
        String awsBucketName = args[0];
        String localWorkingDirectory = args[1];
        String awsPatchDirectory = "patch";
        String patchFileName = "tpp-dds-uploader-patch.jar";

        System.out.println("==========================================");
        System.out.println("   "+APPLICATION_NAME+"                    ");
        System.out.println("==========================================\n");

        try {
            CheckForPatchUpdates(s3.build(),awsPatchDirectory, awsBucketName, localWorkingDirectory, patchFileName);
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

    private static void CheckForPatchUpdates(AmazonS3 s3, String awsPatchDirectory, String awsBucketName, String localWorkingDirectory, String patchFileName) throws SdkBaseException, InterruptedException, IOException
    {
        System.out.println("Checking for patch updates......\n");

        ListObjectsRequest lor = new ListObjectsRequest();
        lor.setBucketName(awsBucketName);
        String patchFileKey = awsPatchDirectory+"/"+patchFileName;
        lor.setPrefix(patchFileKey);
        ObjectListing ol = s3.listObjects(lor);

        // returns the S3 directory and patch files matching key
        List<S3ObjectSummary> patchFiles = ol.getObjectSummaries();
        if (patchFiles.size()>0)
        {
            System.out.println("Patch update available, downloading......\n");
            TransferManagerBuilder tx = TransferManagerBuilder.standard().withS3Client(s3);
            try {
                String patchFile = localWorkingDirectory+awsPatchDirectory+"\\"+patchFileName;
                Download dl = tx.build().download(awsBucketName, patchFileKey, new File(patchFile));
                dl.waitForCompletion();

                // copy the patch file over the existing executable then delete the source file
                PatchLocalFile(localWorkingDirectory, patchFile);
                System.out.println("Patch update applied successfully.\n");

                // remove the source patch file from s3 once successfully patched
                s3.deleteObject(awsBucketName, patchFileKey);
            }
            finally {
                tx.build().shutdownNow();
            }
        }
        else {
            System.out.println("No patch update available.\n");
        }
    }

    private static void PatchLocalFile(String localWorkingDirectory, String patchFileName) throws IOException {
        File patchFile = new File(patchFileName);
        if (Files.exists(patchFile.toPath()))
        {
            String existingFilename = localWorkingDirectory + "tpp-dds-uploader.jar";
            File existingFile = new File(existingFilename);

            Files.copy(patchFile.toPath(), existingFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            Files.delete(patchFile.toPath());
        }
        else
        {
            throw new IOException (String.format("Patch file %s does not exist", patchFileName));
        }
    }
}
