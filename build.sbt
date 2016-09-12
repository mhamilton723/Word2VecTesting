name := "SparkTesting"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.0" //% "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

resolvers += Resolver.sonatypeRepo("public")
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"




//resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
//libraryDependencies += "databricks" % "tensorframes" % "0.2.3-s_2.10"
// META-INF discarding
//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}