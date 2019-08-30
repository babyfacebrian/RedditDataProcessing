import org.apache.spark.sql.{DataFrame, SaveMode}

trait AwsS3Utils {

  // NLP Data locations
  final val submissionsS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/submissions_processed/reddit_submissions_NLP"
  final val commentsS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/comments_processed/reddit_comments_NLP"

  // Agg. Data locations
  final val submissionsAggS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/aggregations_processed/submissions_agg"
  final val commentsAggS3Bucket = "s3a://reddit-data-sentiment-connect/sentiment-data-output/aggregations_processed/comments_agg"

  // Search terms/subreddit data locations
  final val submissionParams = "s3a://reddit-data-sentiment-connect/submission-parameters"
  final val commentsParams = "s3a://reddit-data-sentiment-connect/comment-parameters"

  def loadSubmissionParams(data: DataFrame): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Overwrite)
      .format(source = "csv")
      .option("header", "true")
      .save(this.submissionParams)
  }

  def loadCommentsParams(data: DataFrame): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Overwrite)
      .format(source = "csv")
      .option("header", "true")
      .save(this.commentsParams)
  }

  def loadSubmissionsData(data: DataFrame): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Append)
      .format(source = "json")
      .option("header", "true")
      .save(this.submissionsS3Bucket)
  }

  def loadCommentsData(data: DataFrame, searchTerm: String, subReddit: String): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Append)
      .format(source = "json")
      .option("header", "true")
      .save(this.commentsS3Bucket)
  }

  def loadSubmissionAggData(data: DataFrame): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Append)
      .format(source = "json")
      .option("header", "true")
      .save(this.submissionsAggS3Bucket)
  }

  def loadCommentsAggData(data: DataFrame): Unit = {
    data.coalesce(numPartitions = 1).write.mode(SaveMode.Append)
      .format(source = "json")
      .option("header", "true")
      .save(this.commentsAggS3Bucket)
  }

}
