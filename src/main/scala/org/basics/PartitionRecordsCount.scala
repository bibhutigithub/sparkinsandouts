package org.basics

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object PartitionRecordsCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("header",true).load("src/main/resources/datasets/emp.csv")
    val partitionedDf = df.repartition(4)
    val partitionCount = partitionedDf.rdd.getNumPartitions
    val result_df = partitionedDf.withColumn("partition_id",spark_partition_id())
      .groupBy("partition_id")
      .agg(count(col("EMPNO")).as("empCount"))
    result_df.show()
  }
}
