import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.annotators.ner.NerConverter
import com.johnsnowlabs.nlp.base.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SparkNlpUtils extends SparkSessionWrapper with AwsS3Utils {

  // Spark NLP pipeline Stages
  private val document: DocumentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  private val token: Tokenizer = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  private val normalizer: Normalizer = new Normalizer()
    .setInputCols("token")
    .setOutputCol("normal")

  private val wordEmbeddings: WordEmbeddingsModel = WordEmbeddingsModel.pretrained()
    .setInputCols("document", "token")
    .setOutputCol("word_embeddings")

  private val ner: NerDLModel = NerDLModel.pretrained()
    .setInputCols("normal", "document", "word_embeddings")
    .setOutputCol("ner")

  private val nerConverter: NerConverter = new NerConverter()
    .setInputCols("document", "normal", "ner")
    .setOutputCol("ner_converter")

  private val vivekn: ViveknSentimentModel = ViveknSentimentModel.pretrained()
    .setInputCols("document", "normal")
    .setOutputCol("result_sentiment")

  private val finisher: Finisher = new Finisher()
    .setInputCols("ner", "ner_converter", "result_sentiment")
    .setIncludeMetadata(true)
    .setOutputAsArray(true)
    .setCleanAnnotations(true)


  // Main Spark NLP pipeline
  private val pipeline: Pipeline = new Pipeline()
    .setStages(Array(document, token, normalizer, wordEmbeddings, ner, nerConverter, vivekn, finisher))


  def processNlpData(jsonString: String, dataType: String, searchTerm: String): DataFrame = {

    val data = this.processRawJSON(jsonString, dataType)

    import this.sparkSession.implicits._

    val result = this.pipeline.fit(Seq.empty[String].toDS.toDF("text")).transform(data)
      .withColumn(colName = "entity_type", col = explode(array_except(col("finished_ner"), lit(Array("O")))))
      .withColumn(colName = "sentiment_confidence", col = explode(col("finished_result_sentiment_metadata._2")))
      .withColumn(colName = "sentiment", col = explode(col("finished_result_sentiment")))
      .withColumn(colName = "named_entities", col = explode(array_intersect(col("finished_ner_converter"), lit(Array(searchTerm)))))
      .select("subreddit", "named_entities", "entity_type", "sentiment", "sentiment_confidence")

    // NLP Aggregations
    val nlpData = result.groupBy("subreddit", "named_entities", "entity_type")
      .agg(
        sum(when(col(colName = "sentiment") === "positive", value = 1).otherwise(value = 0)).as(alias = "positive_count"),
        mean(when(col(colName = "sentiment") === "positive", col(colName = "sentiment_confidence")).otherwise(value = 0.0)).as(alias = "positive_confidence_avg"),
        sum(when(col(colName = "sentiment") === "negative", value = 1).otherwise(value = 0)).as("negative_count"),
        mean(when(col(colName = "sentiment") === "negative", col(colName = "sentiment_confidence")).otherwise(value = 0.0)).as(alias = "negative_confidence_avg"))
      .withColumn(colName = "load_ts", col = current_timestamp())

    // Return Final NLP Data
    nlpData
  }


  private def processRawJSON(jsonString: String, dataType: String): DataFrame = {

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
      .select(explode(col(colName = "data"))) // expands json array root
      .select(col = "col.*") // expands col json struct
      .withColumnRenamed(textColumn, newName = "text") // rename title to text

    // Return raw DataFrame
    rawRedditData
  }

}
