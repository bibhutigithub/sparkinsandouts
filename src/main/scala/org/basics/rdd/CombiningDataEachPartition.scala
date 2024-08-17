package org.basics.rdd

import org.apache.spark.sql.SparkSession
import scala.collection.immutable.HashSet

object CombiningDataEachPartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    val normalCollection = Seq(('a',10),('b',15),('a',12),('b',20),('a',20),('b',25),('a',30),('b',45))
    val normalRdd = spark.sparkContext.parallelize(normalCollection,2)
    val resultRdd = normalRdd.aggregateByKey(new HashSet[Int])(_+_,_++_)
    resultRdd.collect.foreach(println)
  }
}
