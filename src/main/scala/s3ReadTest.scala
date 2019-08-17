import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, explode, monotonically_increasing_id}

object s3ReadTest extends App {

  final val access_key: String = new ProfileCredentialsProvider().getCredentials.getAWSAccessKeyId
  final val secret_key: String = new ProfileCredentialsProvider().getCredentials.getAWSSecretKey

  val sparkSession = SparkSession.builder().appName("testing").config("spark.master", "local").getOrCreate()
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", access_key)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret_key)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


  val data = sparkSession.read
    .option("inferSchema", value = true)
    .option("header", value = true)
    .json("s3a://reddit-data-sentiment-connect/sentiment-data-output/submissions_processed/reddit_submissions").toDF()

  data.printSchema()
  data.show(false)
  data.select(count(col("*"))).show()

}
