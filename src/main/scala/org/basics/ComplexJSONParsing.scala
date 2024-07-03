package org.basics

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object ComplexJSONParsing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
  }
}
