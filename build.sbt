name := "SPARK-Piotr-Cichocki"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.14"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % "test"
libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-avro" % "3.2.2",
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql" % "3.2.2"
)
