package Reddit_Data_Preload

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  // todo configure for s3
  final val access_key: String = new ProfileCredentialsProvider().getCredentials.getAWSAccessKeyId
  final val secret_key: String = new ProfileCredentialsProvider().getCredentials.getAWSSecretKey
  lazy val sparkSession: SparkSession = SparkSession.builder().appName("Spark Utils").config("spark.master", "local").getOrCreate()
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", access_key)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret_key)
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
}
