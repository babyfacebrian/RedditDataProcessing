package Reddit_Data_Preload

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, explode}

object SparkAggregationUtils extends SparkSessionWrapper with AwsS3Utils {

  def processAggregationJSON(jsonString: String, dataType: String): DataFrame = {
    val countName = if (dataType.equals("C")) "comment_count" else "submission_count"
    val jsonRDD = this.sparkSession.sparkContext.parallelize(jsonString :: Nil)

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
      .withColumnRenamed("doc_count", countName)
      .withColumnRenamed("key", "subreddit")
      .withColumn("load_ts", current_timestamp())

    // Return formatted data
    formattedAggData
  }

}

case class SparkAggregationProcessing(redditJson: String, fileType: String, searchTerm: String, frequency: String, timeFrame: String) {
  private val redditAggregationData = SparkAggregationUtils.processAggregationJSON(redditJson, fileType)

  def getAggregationData: DataFrame = this.redditAggregationData

  def loadData(): Unit = {
    val fileName = s"FREQ_${frequency}_TIME$timeFrame"

    fileType match {
      case "C" => SparkAggregationUtils.loadCommentsAggData(this.redditAggregationData, fileName)
      case "S" => SparkAggregationUtils.loadSubmissionAggData(this.redditAggregationData, fileName)
      case _ => throw new Exception {
        "File Type is incorrect: Must be C: (comments), or S: (submissions)"
      }
    }
  }
}
