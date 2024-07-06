package org.interviewquestionanswers

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ConsecutiveSeatsAvailable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val dataSeq = Seq((1,1),(2,0),(3,1),(4,1),(5,1)
                      ,(6,0),(7,0),(8,1),(9,1))
    val colsSeq = Seq("seat_id","free")
    val dropColsSeq = Seq("free","lead_val","lag_val")
    val dataDF = dataSeq.toDF(colsSeq:_*)
    //dataDF.show()
    val leadSpec = lead("free",1).over(Window.orderBy(col("seat_id")))
    val lagSpec = lag("free",1).over(Window.orderBy(col("seat_id")))
    val result_df = dataDF.withColumn("lead_val",leadSpec)
                          .withColumn("lag_val",lagSpec)
      .filter(col("free")===1 && (col("lead_val") === 1 || col("lag_val") === 1))
      .drop(dropColsSeq:_*)
      .orderBy(col("seat_id").asc)
    result_df.show()
  }
}
