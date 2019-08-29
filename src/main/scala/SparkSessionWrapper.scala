import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  final val access_key: String = new ProfileCredentialsProvider().getCredentials.getAWSAccessKeyId
  final val secret_key: String = new ProfileCredentialsProvider().getCredentials.getAWSSecretKey

  lazy val sparkSession: SparkSession = SparkSession.builder()
    .appName(name = "NLP Processing")
    .config("spark.driver.memory", "12G")
    .config("spark.kryoserializer.buffer.max", "200M")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  this.sparkSession.sparkContext.setLogLevel("WARN")
  this.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", access_key)
  this.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret_key)
  this.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

}
