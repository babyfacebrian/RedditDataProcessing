import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class JsonObjectRetriever {

    private static void displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }

    public static void main(String[] args) {
        try {
            final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
            String bucketName = "reddit-data-sentiment-connect";
            S3Object fullObject = s3.getObject(bucketName,
                    "sentiment-data-output/comments_processed/reddit_comments_NLP/part-00000-61db969d-90ea-4ce8-b933-a99bc66897d2-c000.json");
            System.out.println(fullObject.getObjectMetadata());
            displayTextInputStream(fullObject.getObjectContent());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
