
object RedditNlpDataApp extends App {

  // Load Submissions
  val submissionParams: Seq[(String, String)] = PreLoadDataUtils.getSubmissionParams
  submissionParams.foreach(param => {
    println(s"SUBMISSIONS STARTING -> TERM: ${param._1} : r/${param._2}")
    loadSubmissions(param._1, param._2, fileType = "S")
    println(s"SUBMISSIONS FINISHED -> TERM: ${param._1} : r/${param._2}")
  })

  // Load Comments
  val commentsParams = PreLoadDataUtils.getCommentsParams
  commentsParams.foreach(param => {
    println(s"COMMENTS STARTING -> TERM: ${param._1} : r/${param._2}")
    loadComments(param._1, param._2, fileType = "C")
    println(s"COMMENTS FINISHED -> TERM: ${param._1} : r/${param._2}")
  })


  private def loadComments(searchTerm: String, subreddit: String, fileType: String): Unit = {
    val jsonData = PushShiftJsonUtils.downloadCommentsJson(searchTerm, subreddit)
    val processor = SparkNlpDataProcessing(jsonData, fileType, searchTerm)
    processor.loadData(searchTerm: String, subreddit: String)
  }

  private def loadSubmissions(searchTerm: String, subreddit: String, fileType: String): Unit = {
    val jsonData = PushShiftJsonUtils.downloadSubmissionsJson(searchTerm, subreddit)
    val processor = SparkNlpDataProcessing(jsonData, fileType, searchTerm)
    processor.loadData(searchTerm: String, subreddit: String)
  }

}
