package Reddit_Data_Preload

import org.apache.spark.sql.DataFrame

case class SparkNlpDataProcessing(redditJson: String, searchTerm: String, fileType: String) {
  // Process Raw JSON data
  private val rawData: DataFrame = SparkNlpUtils.processRawJSON(redditJson, fileType)

  // Entity extraction
  private val nerData: DataFrame = SparkNlpUtils.processNERData(rawData)

  // Sentiment Extraction
  private val sentimentData: DataFrame = SparkNlpUtils.processSentimentData(rawData)

  // Join Entity and Sentiment data
  private val processedData: DataFrame = SparkNlpUtils.joinEntityAndSentimentData(nerData, sentimentData, searchTerm, fileType)

  // Agg data
  private val NlpAggregationData: DataFrame = SparkNlpUtils.processNlpAggregation(processedData)

  def getProcessedData: DataFrame = this.processedData

  def getNLPData: DataFrame = this.NlpAggregationData

  def loadData(): Unit = {
    fileType match {
      case "C" =>
        SparkNlpUtils.loadCommentsData(this.NlpAggregationData)
        println("Comment Data Loaded")
      case "S" =>
        SparkNlpUtils.loadSubmissionsData(this.NlpAggregationData)
        println("Submission Data Loaded")
      case _ => println("File Type is incorrect: Must be C: (comments), or S: (submissions)")
    }
  }

}




/*
case class SparkNlpDataProcessing(redditJson: String, searchTerm: String, fileType: String) {

  private val nlpData = this.createNlpData

  private def createNlpData: DataFrame = {
    val rawData: DataFrame = SparkNlpUtils.processRawJSON(redditJson, fileType)
    val nerData: DataFrame = SparkNlpUtils.processNERData(rawData)
    val sentimentData: DataFrame = SparkNlpUtils.processSentimentData(rawData)
    val processedData: DataFrame = SparkNlpUtils.joinEntityAndSentimentData(nerData, sentimentData, searchTerm, fileType)
    val NlpAggregationData: DataFrame = SparkNlpUtils.processNlpAggregation(processedData)
    NlpAggregationData
  }

  def getNLPData: DataFrame = this.nlpData

  def loadData(): Unit = {
    fileType match {
      case "C" =>
        SparkNlpUtils.loadCommentsData(this.nlpData)
        println("Comment Data Loaded")
      case "S" =>
        SparkNlpUtils.loadSubmissionsData(this.nlpData)
        println("Submission Data Loaded")
      case _ => println("File Type is incorrect: Must be C: (comments), or S: (submissions)")
    }
  }

}
 */
