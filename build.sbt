name := "VideoProcessingApp"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.15.2",
  "com.typesafe.play" %% "play-json" % "2.10.0"
)
