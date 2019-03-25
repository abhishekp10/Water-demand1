name := "spark-bootcamp" // Project name

version := "0.1" // Project version

organization := "nl.rug.sc" // Organization name, used when packaging

scalaVersion := "2.11.12" // Only 2.11.x and 2.10.x are supported

val sparkVersion = "2.4.0" // Latest version

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-core"      % sparkVersion % "provided", // Basic Spark library
  "org.apache.spark" %% "spark-mllib"     % sparkVersion % "provided", // Machine learning library
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided", // Streaming library
  "org.apache.spark" %% "spark-sql"       % sparkVersion % "provided", // SQL library
  "org.apache.spark" %% "spark-graphx"    % sparkVersion % "provided" // Graph library
,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"
   //"datastax" % "spark-cassandra-connector" % "2.2.1-s_2.11"
)