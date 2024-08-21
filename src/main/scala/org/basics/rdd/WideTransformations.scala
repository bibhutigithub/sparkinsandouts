package org.basics.rdd

import org.apache.spark.sql.SparkSession

object WideTransformations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    // Word Count using different wide transformations
    val dataSeq = Seq("Hello Hi", "Hi Folks", "Bye Folks")
    val dataRdd = spark.sparkContext.parallelize(dataSeq)
    val pairedRdd = dataRdd.flatMap(_.split(" "))
      .map((_,1))

    println("Through GroupByKey")
    // Through GroupByKey
    val groupedRdd = pairedRdd.groupByKey.map((groupedData)=>(groupedData._1,groupedData._2.toList.sum))
    groupedRdd.collect.foreach(println)

    println("=====Through ReduceByKey=====")
    // Through ReduceByKey
    val reducedRdd = pairedRdd.reduceByKey(_+_)
    reducedRdd.collect.foreach(println)

    println("=====Through AggregateByKey=====")
    val aggregatedRdd = pairedRdd.aggregateByKey(0)(_+_,_+_)
    aggregatedRdd.collect.foreach(println)

    println("=====Through CombineByKey=====")
    val combinerOps = (occurence:Int) => occurence
    val mergeCombineOps = (accumulator:Int,occurence:Int) => accumulator+occurence
    val combineByKeyRdd = pairedRdd.combineByKey(combinerOps,mergeCombineOps,mergeCombineOps)
    combineByKeyRdd.collect.foreach(println)
  }
}
