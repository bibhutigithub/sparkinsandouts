package org.interviewquestionanswers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object Explode_Operation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val mySchema = StructType(
      List(
         StructField("dept_id",IntegerType)
        ,StructField("eid",ArrayType(IntegerType))
      )
    )
    val df = spark.read
              .format("json")
              .schema(mySchema)
              .option("multiLine", true)
              .load("src/main/resources/datasets/employee.json")
    val result_df = df.withColumn("eid",explode(col("eid")))
    result_df.show()
  }
}
