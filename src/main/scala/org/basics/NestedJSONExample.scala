package org.basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object NestedJSONExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val mySchema = new StructType().add("data",
      StructType(
        List(
          StructField("emp_id", StringType)
          , StructField("emp_name", StringType)
          , StructField("awards", ArrayType(
            StructType(
              List(
                StructField("award_type", StringType)
                , StructField("award_name", StringType)
                , StructField("year", StringType)
              )
            )
           )
          )
         )
       )
     )
    val df = spark.read
      .format("json")
      .schema(mySchema)
      .load("src/main/resources/datasets/employee_awards.json")
    df.select(col("data.emp_id").as("emp_name")
        , col("data.emp_name").as("employee_name")
        , col("data.awards.award_type").as("award_type")
        , col("data.awards.award_name").as("award_name")
        , col("data.awards.year").as("year"))
      .show()
  }
}
