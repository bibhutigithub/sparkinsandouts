package org.basics.rdd

import org.apache.spark.sql.SparkSession

object SharedVariables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    val addByValue = 10
    val dataSeq = Seq(1,2,3,4,5,6,7)
    val dataRdd = spark.sparkContext.parallelize(dataSeq)
    val broadcastVar = spark.sparkContext.broadcast(addByValue)
    val mappedRdd = dataRdd.map(elem=> elem+broadcastVar.value)
    mappedRdd.collect.foreach(println)

    val myAccumulator = spark.sparkContext.longAccumulator("SumAccumulator")
    dataRdd.foreach(elem=> myAccumulator.add(elem))

    println("Accumulator Value is ====")
    println(myAccumulator.value)
  }
}
