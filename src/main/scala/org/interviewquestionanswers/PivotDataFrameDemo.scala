package org.interviewquestionanswers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object PivotDataFrameDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.format("csv").option("header",true).load("src/main/resources/datasets/student_marks.csv")
    val pivotValues = df.select(col("sub")).distinct().collect().map(_(0)).toList
    val final_df = df.groupBy("student")
      .pivot(col("sub"),pivotValues)
      .agg(first(col("marks")))

    final_df.show()

  }
}
