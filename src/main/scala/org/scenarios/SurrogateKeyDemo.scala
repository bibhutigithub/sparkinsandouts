package org.scenarios

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object SurrogateKeyDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val cols = Seq("id","name")
    val dataSeq = Seq((100, "Bibhuti"), (200, "Soumya"), (300, "Plaban"))
    // Below ways to create surrogate keys
    val df = dataSeq.toDF(cols:_*)
      .withColumn("mono_id",monotonically_increasing_id())
      .withColumn("row_id",row_number().over(Window.orderBy(col("id"))))
      .withColumn("md5_key",md5(col("name")))
    df.show()
  }
}
