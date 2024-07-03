package org.basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFrameJSONSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val mySchema = StructType(
      List(
        StructField("name",StringType)
        ,StructField("age",IntegerType)
      )
    )
    val df = spark.read.format("json").schema(mySchema).load("src/main/resources/datasets/employee.json")
    df.show()
  }
}
