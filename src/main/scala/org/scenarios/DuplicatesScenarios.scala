package org.scenarios

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DuplicatesScenarios {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.format("csv")
      .option("header",true)
      .load("src/main/resources/datasets/duplicates_case.csv")

    // Through Group by
    val result_df = df.groupBy("id1","id2","id3")
                      .agg(countDistinct(col("value")).as("valueCount"),first("value").as("value"))
                      .filter(col("valueCount") < 2)
                      .drop("valueCount")
    result_df.show()
  }
}
