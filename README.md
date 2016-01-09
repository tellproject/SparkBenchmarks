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

## Setting parameters

* spark.executor.extraJavaOptions    -Djava.library.path=/mnt/local/marenato/tellbuild/telljava
* spark.executor.extraLibraryPath    /mnt/local/marenato/tellbuild/telljava
* spark.executorEnv.LD_LIBRARY_PATH  /mnt/local/marenato/tellbuild/telljava
* spark.sql.tell.commitmanager       euler10:7242
* spark.sql.tell.storagemanager      euler10:7241;euler11:7241
* spark.driver.extraClassPath        /mnt/local/marenato/tellbuild/telljava/telljava-1.0.jar
* spark.executor.extraClassPath      /mnt/local/marenato/tellbuild/telljava/telljava-1.0.jar
* spark.sql.kudu.master              euler10:XX
* spark.sql.tell.chunkSizeSmall      1073741824
* spark.sql.tell.chunkSizeBig        2073741824 #accounts for ~2gb
* For the shared library you need in master and slaves: <br> ```export LD_LIBRARY_PATH=PATH_TO_TELLBUILD_TELLJAVA```

## For running it
The uber jar created from steps before is: ```StorageEngBenchmark.jar```

* Start spark master: ```PATH_TO_SPARK/sbin/start-master.sh```
* Start spark slave(s): ```PATH_TO_SPARK/sbin/start-slave.sh spark://MASTER_IP:7077```
* Submit the job:
<br>```PATH_TO_SPARK/bin/spark-submit --class ch.ethz.QueryRunner --master spark://euler10:7077 /home/marenato/StorageEngBenchmark.jar tell chb```
* Options for storage backend: ```tell``` for TellStore, ```kudu``` for Kudu
* Options for benchmark: ```chb``` for CH benchmark, ```tpch``` for TPCH benchmark