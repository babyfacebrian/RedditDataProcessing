package Reddit_Data_Preload

import org.apache.spark.ml.PipelineModel

trait PreTrainedNlpWrapper {
  // todo change paths!
  final val entityModelPath: String = "/Users/briankalinowski/IdeaProjects/ScalaSparkPlaygroud/src/NLP_pretrained/recognize_entities_dl_en_2.1.0_2.4_1562946909722"
  final val sentimentModelPath: String = "/Users/briankalinowski/IdeaProjects/ScalaSparkPlaygroud/src/NLP_pretrained/analyze_sentiment_en_2.1.0_2.4_1563204637489"
  lazy val NERPipeline: PipelineModel = PipelineModel.load(this.entityModelPath)
  lazy val sentimentPipeLine: PipelineModel = PipelineModel.load(this.sentimentModelPath)
}
