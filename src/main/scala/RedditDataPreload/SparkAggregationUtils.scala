package RedditDataPreload

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, explode, lit}

object SparkAggregationUtils extends SparkSessionWrapper with AwsS3Utils {

  def processAggregationJSON(jsonString: String, searchTerm: String, dataType: String, frequency: String, timeFrame: String): DataFrame = {

    val countName: String = if (dataType.equals("C")) "comment_count" else "submission_count"
    val jsonRDD: RDD[String] = this.sparkSession.sparkContext.parallelize(jsonString :: Nil)
    val searchTermColumn: String = searchTerm.trim.toLowerCase

    // Read raw json RDD
    val rawAggData = this.sparkSession.read
      .option("inferSchema", value = true) // infer json schema
      .option("header", value = true) // header columns
      .option("multiLine", value = true) // multiline option
      .option("mode", "DROPMALFORMED") // drops any mal-formatted json records
      .json(jsonRDD).toDF

    val formattedAggData = rawAggData
      .select("aggs.*")
      .select(explode(col("subreddit")))
      .select("col.*")
      .withColumnRenamed("key", "subreddit")
      .withColumnRenamed("doc_count", countName)
      .withColumn("search_term", lit(searchTermColumn))
      .withColumn("frequency", lit(frequency))
      .withColumn("time_frame", lit(timeFrame))
      .withColumn("load_ts", current_timestamp())

    // Return formatted data
    formattedAggData
  }

}

case class SparkAggregationProcessing(redditJson: String, searchTerm: String, fileType: String, frequency: String, timeFrame: String) {

  private val redditAggregationData = SparkAggregationUtils.processAggregationJSON(redditJson, searchTerm, fileType, frequency, timeFrame)

  def getAggregationData: DataFrame = this.redditAggregationData

  def loadData(): Unit = {
    fileType match {
      case "C" =>
        SparkAggregationUtils.loadCommentsAggData(this.redditAggregationData)
        println("Comment Data Loaded")
      case "S" =>
        SparkAggregationUtils.loadSubmissionAggData(this.redditAggregationData)
        println("Submission Data Loaded")
      case _ => println("File Type is incorrect: Must be C: (comments), or S: (submissions)")
    }
  }
}
