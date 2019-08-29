package RedditDataPreload


object RedditNlpDataApp extends App {

  args.length match {
    case 0 => println("No args Program stopped")
    case 3 =>
      println(s"3 args: ${args(0)}, ${args(1)}, ${args(2)}")
      runDataPreLoad(args(0), args(1), args(2))
    case _ => println("ERROR: Args must be: searchTerm -> subreddit -> (C or S)")
  }

  def loadComments(searchTerm: String, subreddit: String, fileType: String): Unit = {
    val jsonData = PushShiftJsonUtils.downloadCommentsJson(searchTerm, subreddit)
    val processor = SparkNlpDataProcessing(jsonData, fileType, searchTerm)
    processor.loadData(searchTerm: String, subreddit: String)
  }

  def loadSubmissions(searchTerm: String, subreddit: String, fileType: String): Unit = {
    val jsonData = PushShiftJsonUtils.downloadSubmissionsJson(searchTerm, subreddit)
    val processor = SparkNlpDataProcessing(jsonData, fileType, searchTerm)
    processor.loadData(searchTerm: String, subreddit: String)
  }

  def runDataPreLoad(searchTerm: String, subreddit: String, fileType: String): Unit = {
    fileType match {
      case "C" => loadComments(searchTerm, subreddit, fileType)
      case "S" => loadSubmissions(searchTerm, subreddit, fileType)
      case _ => println("Error type must be S or C")
    }
  }

}
