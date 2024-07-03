package org.basics

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object MapTypeColumnDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val dataSeq = Seq((100, "Bibhuti", Map("pupil"->"black","nails"->"white")), (200, "Soumya",Map("pupil"->"brown","nails"->"yellow")), (300, "Plaban",Map("pupil"->"green","nails"->"white")))
    val df = dataSeq.toDF("id","name","features")
    val result_df = df.withColumn("pupil",col("features").getItem("pupil"))
      .withColumn("nails",col("features").getItem("nails"))
      .drop("features")
    result_df.show()
  }
}
