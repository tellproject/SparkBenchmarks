import sbt.Resolver

assemblyJarName in assembly := "StorageEngBenchmark.jar"

organization := "ch.ethz"

name := "StorageEngBenchmark"

version := "1.0"

scalaVersion := "2.10.5"

mainClass in Compile := Some("ch.ethz.QueryRunner")

resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.file("Local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
)

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "3.0.0-SNAP4",
  "org.apache.spark" % "spark-core_2.10" % "1.6.0", //exclude("org.slf4j", "slf4j-api")
  "org.apache.spark" % "spark-sql_2.10" % "1.6.0",
  "ch.ethz.tell" % "tell-spark_2.10" % "1.0",
  "org.apache.kudu" % "kudu-spark" % "14",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0"
)

assemblyMergeStrategy in assembly := {
  case x if x.startsWith("META-INF") => MergeStrategy.discard // Bumf
  case x if x.endsWith(".html") => MergeStrategy.discard // More bumf
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x if x.contains("org/cyberneko/html") => MergeStrategy.first
  case x if x.contains("SingleThreadModel.class") => MergeStrategy.first
  case x if x.contains("javax.servlet") => MergeStrategy.first
  case x if x.contains("org.eclipse") => MergeStrategy.first
  case x if x.contains("org.apache") => MergeStrategy.first
  case x if x.contains("org.slf4j") => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs@_ *) => MergeStrategy.last // For Log$Logger.class
  case x => MergeStrategy.first
}
