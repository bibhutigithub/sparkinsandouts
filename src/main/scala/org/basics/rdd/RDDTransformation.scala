package org.basics.rdd

import org.apache.spark.sql.SparkSession

object RDDTransformation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    val wordRdd = spark.sparkContext.textFile("src/main/resources/datasets/word_counts.txt", 3)
    val resultRdd = wordRdd.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)
    resultRdd.collect.foreach(println)
  }
}
