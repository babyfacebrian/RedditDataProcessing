package Reddit_Data_Preload

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object SparkNlpUtils extends SparkSessionWrapper with PreTrainedNlpWrapper with AwsS3Utils {

  def processRawJSON(jsonString: String, dataType: String): DataFrame = {

    val textColumn: String = {
      if (dataType.equals("C")) "body" else if (dataType.equals("S")) "title" else throw new Exception
    }

    val jsonRDD: RDD[String] = this.sparkSession.sparkContext.parallelize(jsonString :: Nil)

    // Read raw json RDD
    val rawRedditData: DataFrame = this.sparkSession.read
      .option("inferSchema", value = true) // infer json schema
      .option("header", value = true) // header columns
      .option("multiLine", value = true) // multiline option
      .option("mode", "DROPMALFORMED") // drops any mal-formatted json records
      .json(jsonRDD).toDF

    // Format data
    val formattedRedditData: DataFrame = rawRedditData
      .select(explode(col("data"))) // expands json array root
      .select("col.*") // expands col json struct
      .select("subreddit", textColumn) // select needed columns
      .withColumnRenamed(textColumn, "text") // rename title to text
      .withColumn("prime_id", monotonically_increasing_id()) // adds an increasing id to each row

    // Return formatted raw DataFrame
    formattedRedditData
  }


  def processNERData(data: DataFrame): DataFrame = {
    val NERData = this.NERPipeline.transform(data) // transform with NER model
      .select("prime_id", "subreddit", "entities.result")
      .withColumnRenamed("result", "named_entity")
      .orderBy("prime_id")
    NERData
  }


  def processSentimentData(data: DataFrame): DataFrame = {
    val sentimentData = this.sentimentPipeLine.transform(data) // transform with Sentiment model
      .select("prime_id", "subreddit", "sentiment.result", "sentiment.metadata")
      .withColumnRenamed("result", "sentiment")
      .withColumnRenamed("metadata", "sentiment_confidence")
      .orderBy("prime_id")
    sentimentData
  }


  def joinAndLoadProcessedData(nerData: DataFrame, sentimentData: DataFrame, searchTerm: String): DataFrame = {

    // Format the search term for filtering
    val searchTermRegex = s"${searchTerm.trim.toLowerCase}|${searchTerm.split(" ").map(_.capitalize).mkString(" ")}"

    val processedRedditData = nerData.join(sentimentData, Seq("prime_id", "subreddit"), "inner")
      .withColumn("named_entity", explode(col("named_entity"))) // expand named entities array
      .withColumn("sentiment", explode(col("sentiment"))) // expand sentiment array
      .withColumn("sentiment_confidence", explode(col("sentiment_confidence"))) // expand sentiment confidence array
      .withColumn("sentiment_confidence", col("sentiment_confidence").getField("confidence")) // get confidence value
      .withColumn("load_ts", current_timestamp()) // add timestamp column


    // Return processed data
    processedRedditData.filter(col("named_entity").rlike(searchTermRegex)) // filter out entities to better match the search term/phrase
      .drop("prime_id")
      .orderBy("named_entity")
  }


  // todo Fix this it needs to be updated
  def processNlpAggregation(processedData: DataFrame): DataFrame = {
    processedData.groupBy("subreddit", "named_entity").agg(
      sum(when(col("sentiment") === "positive", 1).otherwise(0)).as("positive_count"), // sum of positive sentences
      mean(when(col("sentiment") === "positive", col("sentiment_confidence")).otherwise(0.0)).as("positive_confidence_avg"),
      sum(when(col("sentiment") === "negative", 1).otherwise(0)).as("negative_count"), // sum of negative sentences
      mean(when(col("sentiment") === "negative", col("sentiment_confidence")).otherwise(0.0)).as("negative_confidence_avg"))
      .withColumn("load_ts", current_timestamp())
      .orderBy(desc("positive_count"), desc("positive_confidence_avg"))
  }

}
