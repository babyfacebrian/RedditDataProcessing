package Reddit_Data_Preload

import org.apache.spark.sql.functions.{col, count}

object PreLoadDataApp extends App {

  val jsonTest = PushShiftJsonUtils.downloadSubmissionsJson("Trump", "politics")

  val sparkProcessor: SparkNlpDataProcessing = SparkNlpDataProcessing(jsonTest, "S", "Trump")

  val redditData = sparkProcessor.getProcessedData

  redditData.printSchema()
  redditData.show(false)
  redditData.select(count(col("*"))).show()


  //sparkProcessor.loadSubmissionsData(redditData)




}
