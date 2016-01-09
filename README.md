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

## For running it
The uber jar created from steps before is: ```StorageEngBenchmark.jar```

* Start spark master: ```PATH_TO_SPARK/sbin/start-master.sh```
* Start spark slave(s): ```PATH_TO_SPARK/sbin/start-slave.sh spark://MASTER_IP:7077```
* Submit the job:
<br>```PATH_TO_SPARK/bin/spark-submit --class ch.ethz.QueryRunner --master spark://euler10:7077 /home/marenato/StorageEngBenchmark.jar tell chb```
* Options for storage backend: ```tell``` for TellStore, ```kudu``` for Kudu
* Options for benchmark: ```chb``` for CH benchmark, ```tpch``` for TPCH benchmark