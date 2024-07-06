package org.scenarios

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ReadAllFilesNestedFolders {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read
              .format("csv")
              .option("header",true)
              .option("recursiveFileLookup", "true")
              .load("src/main/resources/datasets/nested")
    print(df.count())
  }
}
