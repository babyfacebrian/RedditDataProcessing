
object RedditNlpDataApp extends App {


  // Load Submissions
  val submissionParams: Seq[(String, String)] = PreLoadDataUtils.getSubmissionParams
  submissionParams.foreach(param => {
    println(s"SUBMISSIONS STARTING -> TERM: ${param._1} : r/${param._2}")
    loadSubmissions(param._1, param._2, fileType = "S")
    println(s"SUBMISSIONS FINISHED -> TERM: ${param._1} : r/${param._2}")
  })


  // Load Comments
  val commentsParams: Seq[(String, String)] = PreLoadDataUtils.getCommentsParams
  commentsParams.foreach(param => {
    println(s"COMMENTS STARTING -> TERM: ${param._1} : r/${param._2}")
    loadComments(param._1, param._2, fileType = "C")
    println(s"COMMENTS FINISHED -> TERM: ${param._1} : r/${param._2}")
  })


  private def loadComments(searchTerm: String, subreddit: String, fileType: String): Unit = {
    val jsonData = PushShiftJsonUtils.downloadCommentsJson(searchTerm, subreddit)

    // Check for blank comments
    if (jsonData.contains("body")) {
      val processor = SparkNlpDataProcessing(jsonData, fileType, searchTerm)
      processor.loadData(searchTerm: String, subreddit: String)
    } else {
      println(s"No Comments found for $searchTerm in r/$subreddit")
    }
  }

  private def loadSubmissions(searchTerm: String, subreddit: String, fileType: String): Unit = {
    val jsonData = PushShiftJsonUtils.downloadSubmissionsJson(searchTerm, subreddit)

    // Check for blank submissions
    if (jsonData.contains("title")) {
      val processor = SparkNlpDataProcessing(jsonData, fileType, searchTerm)
      processor.loadData(searchTerm: String, subreddit: String)
    } else {
      println(s"No Submissions found for $searchTerm in r/$subreddit")
    }
  }

}
