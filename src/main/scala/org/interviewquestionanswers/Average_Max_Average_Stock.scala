package org.interviewquestionanswers

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, max}

object Average_Max_Average_Stock {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val dataSeq = Seq(("AAPL", 150, "2023-01-01 09:45:50")
                      ,("AAPL", 153, "2023-01-01 11:45:50")
                      ,("AAPL", 158, "2023-01-01 14:45:50")
                      ,("AAPL", 160, "2023-01-02 09:45:50")
                      ,("AAPL", 166, "2023-01-02 11:45:50")
                      ,("AAPL", 170, "2023-01-02 14:45:50")
                      ,("GOOG", 2500, "2023-01-01 14:45:50")
                      ,("GOOG", 2550, "2023-01-02 14:45:50")
                      ,("MSFT", 300, "2023-01-01 14:45:50")
                      ,("MSFT", 310, "2023-01-02 14:45:50"))

    val stock_df = dataSeq.toDF("stock","price","date")
      .withColumn("date", col("date").cast("date")
      )
    val avg_stock_val = stock_df.groupBy("stock", "date").agg(avg("price").alias("avg_price"))
      .withColumn("avg_price",col("avg_price").cast("decimal(12,2)"))
    avg_stock_val.show()
    val max_avg_value_stock_df = avg_stock_val.groupBy("stock").agg(max("avg_price"))
    max_avg_value_stock_df.show()
  }
}
