package Reddit_Data_Preload

import org.apache.spark.sql.DataFrame

case class SparkNlpDataProcessing(redditJson: String, fileType: String, searchTerm: String) {

  // Process Raw JSON data
  private val rawData: DataFrame = SparkNlpUtils.processRawJSON(redditJson, fileType)

  // Entity extraction
  private val nerData: DataFrame = SparkNlpUtils.processNERData(rawData)

  // Sentiment Extraction
  private val sentimentData: DataFrame = SparkNlpUtils.processSentimentData(rawData)

  // Join Entity and Sentiment data
  private val processedData: DataFrame = SparkNlpUtils.joinAndLoadProcessedData(nerData, sentimentData, searchTerm)

  // Aggregation data
  private val NlpAggregationData: DataFrame = SparkNlpUtils.processNlpAggregation(processedData)

  def getProcessedData: DataFrame = this.processedData

  def getNLPData: DataFrame = this.NlpAggregationData

  def loadRedditSubmissions(data: DataFrame): Unit = {
    SparkNlpUtils.loadSubmissionsData(data)
  }

  def loadRedditComments(data: DataFrame): Unit = {
    SparkNlpUtils.loadCommentsData(data)
  }

  def loadRedditSubmissionAggregations(data: DataFrame): Unit = {
    SparkNlpUtils.loadSubmissionAggData(data)
  }

  def loadRedditCommentAggregations(data: DataFrame): Unit = {
    SparkNlpUtils.loadCommentsAggData(data)
  }

}
