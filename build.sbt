name := "sparkml-flight-delay"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "com.databricks" %% "spark-csv" % "1.3.0",
  "com.typesafe" % "config" % "1.3.0",
  "com.github.mdr" % "ascii-graphs_2.10" % "0.0.3",
  "com.github.tototoshi" % "scala-csv_2.10" % "1.2.2",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)
