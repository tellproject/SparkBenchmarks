# SparkBenchmarks

The project has two dependencies that you must install first:

1. "ch.ethz.tell" % "tell-spark_2.10" % "1.0"
2. "org.apache.kudu" % "kudu-spark" % "14"

For 1:
* git clone https://github.com/tellproject/TellSpark.git
* cd TellSpark/
* sbt compile package (not mandatory)
* sbt publishLocal

For 2:
* git clone https://github.com/renato2099/SparkOnKudu.git
* mvn clean install -DskipTests

Then you can get your assembly the usual way:
* sbt clean assembly