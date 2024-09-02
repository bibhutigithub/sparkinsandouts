package org.basics.rdd

import org.apache.spark.sql.SparkSession

object SortingRDDs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    val dataSeq = Seq(("Bibhuti",100),("Soumya",200),("Plaban",300), ("Prashant",400))
    val dataRdd = spark.sparkContext.parallelize(dataSeq)
    val sortedRdd = dataRdd.sortByKey(false)
    sortedRdd.collect.foreach(println)

    val sortByAnyField = dataRdd.sortBy(elem=>elem._2, false)
    sortByAnyField.collect.foreach(println)
  }
}
