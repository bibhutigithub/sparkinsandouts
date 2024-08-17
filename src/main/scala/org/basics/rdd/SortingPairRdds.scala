package org.basics.rdd

import org.apache.spark.sql.SparkSession

object SortingPairRdds {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    val dataSeq = Seq("Hello World", "Say Hello", "Hi World", "Take Care")
    val dataRdd = spark.sparkContext.parallelize(dataSeq)
    val resultRdd = dataRdd.flatMap(line=>line.split(" ")).map(word=> (word,1))
      .sortByKey(true)
    resultRdd.collect.foreach(println)
  }
}
