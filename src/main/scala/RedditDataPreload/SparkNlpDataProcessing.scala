package RedditDataPreload

import org.apache.spark.sql.DataFrame


case class SparkNlpDataProcessing(redditJson: String, fileType: String, searchTerm: String) {

  private val nlpData = SparkNlpUtils.processNlpData(redditJson, fileType, searchTerm)

  def getNLPData: DataFrame = this.nlpData

  def loadData(searchTerm: String, subReddit: String): Unit = {
    fileType match {
      case "C" =>
        SparkNlpUtils.loadCommentsData(this.nlpData, searchTerm: String, subReddit: String)
        println("Comment Data Loaded")
      case "S" =>
        SparkNlpUtils.loadSubmissionsData(this.nlpData)
        println("Submission Data Loaded")
      case _ => println("File Type is incorrect: Must be C: (comments), or S: (submissions)")
    }
  }
}




