package Reddit_Data_Preload

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}


object PreLoadDataApp extends App {

  val jsonTest: String = PushShiftJsonUtils.downloadCommentsJson("Trump", "politics")

  val processor: SparkNlpDataProcessing = SparkNlpDataProcessing(jsonTest, "Trump", "C")

  val data = processor.getNLPData
  data.show(false)
  data.select(count(col("*"))).show()


//  //processor.loadData()



}
