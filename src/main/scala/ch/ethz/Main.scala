package ch.ethz

import ch.ethz.tell.TellContext
import org.apache.spark.{SparkConf, SparkContext}

object Application {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val context = new TellContext(sc)
    context.startTransaction()

    val data = context.read.format("tell").options(Map(
      "table" -> "testTable",
      "numPartitions" -> "8"
    )).load()
    data.filter(data("number") >= 14).select("text2", "number").show(250)

    context.commitTransaction()
  }
}
