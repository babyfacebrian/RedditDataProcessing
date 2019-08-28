import Reddit_Data_Preload.{AwsS3Utils, SparkSessionWrapper}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object s3ReadTest extends App with SparkSessionWrapper with AwsS3Utils {

  def getCommentsData(searchTerm: String, subReddit: String): DataFrame = {
    this.sparkSession.read
      .option("inferSchema", value = true)
      .option("header", value = true)
      .json(this.commentsS3Bucket).toDF()
      .where(col("named_entities").rlike(searchTerm) && col("subreddit") === subReddit)
  }

  def getSubmissionsData(searchTerm: String, subReddit: String): DataFrame = {
    this.sparkSession.read
      .option("inferSchema", value = true)
      .option("header", value = true)
      .json(this.submissionsS3Bucket).toDF()
      .where(col("named_entities").rlike(searchTerm) && col("subreddit") === subReddit)
  }

  val test = getCommentsData("Trump", "politics")
  test.show()



}
