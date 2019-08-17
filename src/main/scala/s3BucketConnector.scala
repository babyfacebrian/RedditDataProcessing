import java.io.File

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder

object s3BucketConnector extends App {


  val bucket = "reddit-data-sentiment-connect/pushshift-data-input"

  val testFile = new File("/Users/briankalinowski/Desktop/RedditDataProcessing/src/data/reddit_data_test.json")

  val s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build()

  s3.putObject(bucket, "test.json", testFile)


}
