package org.basics.rdd

import org.apache.spark.sql.SparkSession

object RDDExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    val normalCollection = Seq(10, 12, 13, 14, 15, 16)
    val paralleliseCollection = spark.sparkContext.parallelize(normalCollection, 2)

    val transformedRdd = paralleliseCollection.mapPartitionsWithIndex { (partNum, partData) => {
      partData.map(elem=>elem*2)
      }
    }
    transformedRdd.collect.foreach(println)
  }
}
