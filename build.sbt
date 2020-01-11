name := "alon-test"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= {
  val sparkV = "2.4.4"
  Seq(
    // spark
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,

    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.703"
  )
}