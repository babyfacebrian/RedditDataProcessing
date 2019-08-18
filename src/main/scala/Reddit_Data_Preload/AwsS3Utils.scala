package Reddit_Data_Preload

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.sql.DataFrame

trait AwsS3Utils {

  private final val submissionsS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/submissions_processed/reddit_submissions"
  private final val commentsS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/comments_processed/reddit_comments"
  private final val submissionsAggS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/aggregations_processed/submissions_agg"
  private final val commentsAggS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/aggregations_processed/comments_agg"

  private val S3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build()


  def loadSubmissionsData(data: DataFrame): Unit = {
    data.coalesce(1).write.format("json").option("header", "true").save(this.submissionsS3Bucket)
  }

  def loadCommentsData(data: DataFrame): Unit = {
    data.coalesce(1).write.format("json").option("header", "true").save(this.commentsS3Bucket)
  }

  def loadSubmissionAggData(data: DataFrame, fileName: String): Unit = {
    data.coalesce(1).write.format("json").option("header", "true").save(this.submissionsAggS3Bucket + "/" + fileName)
  }

  def loadCommentsAggData(data: DataFrame, fileName: String): Unit = {
    data.coalesce(1).write.format("json").option("header", "true").save(this.commentsAggS3Bucket + "/" + fileName)
  }

  def clearSubmissionsData(): Unit = this.S3Client.deleteObject(this.submissionsS3Bucket, "reddit_submissions")

  def clearCommentsData(): Unit = this.S3Client.deleteObject(this.commentsS3Bucket, "reddit_comments")

  def clearSubmissionsAggData(): Unit = this.S3Client.deleteObject(this.submissionsAggS3Bucket, "submissions_agg")

  def clearCommentsAggData(): Unit = this.S3Client.deleteObject(this.commentsAggS3Bucket, "comments_agg")

}
