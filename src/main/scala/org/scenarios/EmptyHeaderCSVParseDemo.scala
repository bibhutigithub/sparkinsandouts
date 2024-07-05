package org.scenarios
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object EmptyHeaderCSVParseDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    val df = spark.read.text("src/main/resources/datasets/emp_empty_headers.csv")
    val result_df = df.withColumn("row_id",monotonically_increasing_id())
    var data_df = result_df.filter(col("row_id")>2)
                  .drop("row_id")
                  .withColumn("value",split(col("value"),","))

    val headerColList = result_df
      .filter(col("row_id")===2)
      .drop("row_id")
      .select("value")
      .collect()(0)(0).toString.split(",")

    for((colName, i) <- headerColList.zipWithIndex) {
      data_df = data_df.withColumn(colName,col("value")(i))
    }
    data_df
      .drop("value")
      .show()
  }
}
