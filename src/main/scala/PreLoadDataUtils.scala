
object PreLoadDataUtils extends SparkSessionWrapper with AwsS3Utils {

  def getSubmissionParams: Seq[(String, String)] = this.sparkSession.read
    .option("inferSchema", value = true)
    .option("header", value = true)
    .csv(this.submissionParams)
    .rdd.map(param => (param(0).toString, param(1).toString)).collect().toSeq


  def getCommentsParams: Seq[(String, String)] = this.sparkSession.read
    .option("inferSchema", value = true)
    .option("header", value = true)
    .csv(this.commentsParams)
    .rdd.map(param => (param(0).toString, param(1).toString)).collect().toSeq

}
