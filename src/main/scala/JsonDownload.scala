import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import scala.io.Source


object JsonDownload extends App {

  val pushshiftURL = "https://api.pushshift.io/reddit/search/submission/?q=trump&subreddit=politics&fields=subreddit,title&after=24h&size=500"
  val jsonDataConnection = Source.fromURL(pushshiftURL)
  val redditJsonString = jsonDataConnection.mkString.toString.stripLineEnd
  jsonDataConnection.close()

  val sparkSession = SparkSession.builder().appName("json testing").config("spark.master", "local").getOrCreate()
  val redditRDD = sparkSession.sparkContext.parallelize(redditJsonString :: Nil)

  val redditDf = sparkSession.read
    .option("inferSchema", value = true)
    .option("header", value = true)
    .json(redditRDD)
    .select(explode(col("data")))
    .select("col.*")

  redditDf.printSchema()
  redditDf.show(20)

}
