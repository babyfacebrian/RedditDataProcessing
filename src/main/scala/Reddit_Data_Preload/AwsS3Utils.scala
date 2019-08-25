package Reddit_Data_Preload

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.sql.{DataFrame, SaveMode}

trait AwsS3Utils {

  final val submissionsS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/submissions_processed/reddit_submissions_NLP"
  final val commentsS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/comments_processed/reddit_comments_NLP"
  final val submissionsAggS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/aggregations_processed/submissions_agg"
  final val commentsAggS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/aggregations_processed/comments_agg"


  def loadSubmissionsData(data: DataFrame): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Append)
      .format("json")
      .option("header", "true")
      .save(this.submissionsS3Bucket)
  }

  def loadCommentsData(data: DataFrame, searchTerm: String, subReddit: String): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Append)
      .format("json")
      .option("header", "true")
      .save(this.commentsS3Bucket)
  }

  def loadSubmissionAggData(data: DataFrame): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Append)
      .format("json")
      .option("header", "true")
      .save(this.submissionsAggS3Bucket)
  }

  def loadCommentsAggData(data: DataFrame): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Append)
      .format("json")
      .option("header", "true")
      .save(this.commentsAggS3Bucket)
  }

}
