package org.scenarios

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, date_diff, first, lag, last, lead, lit, row_number, to_date, when,sum}

object SeriesEventDates {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val dataSeq = Seq(("2020-06-01","Won"),("2020-06-02","Won")
                      ,("2020-06-03","Won"),("2020-06-04","Lost")
                      ,("2020-06-05","Lost"),("2020-06-06","Lost")
                      ,("2020-06-07","Won"))

    val dataDF = dataSeq.toDF("event_date","event_status")
      .withColumn("event_date", col("event_date").cast("date"))

    val finalDF = dataDF
                    .withColumn("lag_event_status",lag(col("event_status"), 1).over(Window.orderBy(col("event_date"))))
                    .withColumn("column_status",when(col("event_status") === col("lag_event_status") || col("lag_event_status").isNull,lit(0)).otherwise(lit(1)))
                    .withColumn("running_sum",sum(col("column_status")).over(Window.orderBy(col("event_date"))))
                    .groupBy(col("running_sum"))
                    .agg(first(col("event_status")).as("event_status")
                      ,first(col("event_date")).as("event_start_date")
                      , last(col("event_date")).as("event_end_date"))
                    .drop("running_sum")
    finalDF.show()
  }
}
