name := "RedditDataProcessing"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.2.0"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.375"

dependencyOverrides += "org.apache.hadoop" % "hadoop-common" % "3.2.0"

dependencyOverrides += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.0"
