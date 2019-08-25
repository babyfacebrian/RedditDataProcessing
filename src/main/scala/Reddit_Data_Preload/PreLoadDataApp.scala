package Reddit_Data_Preload


object PreLoadDataApp extends App {

  val democraticRunners = List("Michael Bennet", "Cory Booker",
    "Steve Bullock", "Pete Buttigieg", "Julian Castro", "John Delaney", "Tulsi Gabbard",
    "Kristen Gillibrand", "Kamala Harris", "Amy Klobuchar", "Wayne Messam", "Beto O'Rourke",
    "Tim Ryan", "Bernie Sanders", "Joe Sestak", "Tom Steyer", "Marianne Williamson")

//  democraticRunners.foreach(name => {
//    println(s"NAME: $name")
//    runDataPreLoad(name, "politics", "S")
//    println(s"FINISHED: $name")
//  })

//  democraticRunners.foreach(name => {
//    println(s"STARTING: $name")
//    runDataPreLoad(name, "politics", "C")
//    println(s"FINISHED: $name")
//  })

  runDataPreLoad(searchTerm = "Trump", subreddit = "politics", fileType = "C")

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
