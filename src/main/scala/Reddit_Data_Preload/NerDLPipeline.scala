package Reddit_Data_Preload

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.annotators.ner.NerConverter
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.util.Benchmark
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object NerDLPipeline extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.driver.memory", "12G")
    .config("spark.kryoserializer.buffer.max", "200M")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val document = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  val token = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  val normalizer = new Normalizer()
    .setInputCols("token")
    .setOutputCol("normal")

  val wordEmbeddings = WordEmbeddingsModel.pretrained()
    .setInputCols("document", "token")
    .setOutputCol("word_embeddings")

  val ner = NerDLModel.pretrained()
    .setInputCols("normal", "document", "word_embeddings")
    .setOutputCol("ner")

  val nerConverter = new NerConverter()
    .setInputCols("document", "normal", "ner")
    .setOutputCol("ner_converter")

  val vivekn = ViveknSentimentModel.pretrained()
    .setInputCols("document", "normal")
    .setOutputCol("result_sentiment")

  val finisher = new Finisher()
    .setInputCols("ner", "ner_converter", "result_sentiment")
    .setIncludeMetadata(true)
    .setOutputAsArray(true)
    .setCleanAnnotations(true)

  val pipeline = new Pipeline().setStages(Array(document, token, normalizer, wordEmbeddings, ner, nerConverter, vivekn, finisher))

  val jsonTest: String = PushShiftJsonUtils.downloadCommentsJson("Trump", "politics")

  val data: DataFrame = SparkNlpUtils.processRawJSON(jsonTest, "C")

  val result = pipeline.fit(Seq.empty[String].toDS.toDF("text")).transform(data)
    .withColumn("finished_ner", array_except(col("finished_ner"), lit(Array("O"))))
    .withColumn("finished_result_sentiment_metadata", explode(col("finished_result_sentiment_metadata")))
    .withColumn("finished_result_sentiment", explode(col("finished_result_sentiment")))
    .where(array_contains(col("finished_ner_converter"), "Trump") || array_contains(col("finished_ner_converter"), "trump"))

  val resultFormatted = result.select("subreddit", "text", "finished_ner", "finished_ner_converter", "finished_result_sentiment", "finished_result_sentiment_metadata._2")
    .withColumnRenamed("finished_ner", "ner_type")
    .withColumnRenamed("finished_ner_converter", "named_entities")
    .withColumnRenamed("finished_result_sentiment", "sentiment")
    .withColumnRenamed("_2", "sentiment_confidence")

  Benchmark.time("Time to convert and show") {
    resultFormatted.printSchema()
    resultFormatted.show()
    println("COUNT: " + resultFormatted.count())
  }

}
