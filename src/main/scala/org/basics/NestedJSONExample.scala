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

    val df_result = df.withColumn("awards",explode(col("data.awards")))
      .select("data.emp_id"
              ,"data.emp_name"
              ,"awards.*"
              )
    df_result.show()
  }
}
