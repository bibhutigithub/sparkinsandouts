package org.scenarios

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object DataProfilingNullCountsOnColumns {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val cols = Seq("id", "name")
    val dataSeq = Seq((Some(100), Some("Bibhuti")), (Some(101), Some("Soumya"))
      , (None, Some("Plaban")), (Some(102), None)
      , (Some(103), Some("Ajit")), (Some(104), None))
    val df = dataSeq.toDF(cols: _*)

    val countColumns = df.columns.map{colName=>{
      count(when(col(colName).isNull,true)).as(colName+"_null_count")
    }
    }
    val final_df = df.select(countColumns:_*)
    final_df.show()
  }
}
