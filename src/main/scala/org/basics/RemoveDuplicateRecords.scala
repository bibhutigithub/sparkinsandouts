package org.basics

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RemoveDuplicateRecords {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.format("csv")
                  .option("header",true)
                  .load("src/main/resources/datasets/emp_duplicates.csv")

    //df.show()
    // One way
    val result_df = df.orderBy(col("updated_date").desc).dropDuplicates(Seq("id","name"))
    //result_df.show()

    // Through window functions
    val partitionWindow = Window.partitionBy("id","name").orderBy(col("updated_date").desc)
    val result_final_df = df.withColumn("row_id",row_number().over(partitionWindow))
                            .filter(col("row_id")===1)
                            .drop("row_id")
    //result_final_df.show()

    // Through Group BY
    val colOperationsMap:Map[String,Column=>Column] = Map("id"-> count, "designation"->first, "updated_date"->first)
    val colAliasMap = Map("id"->"colCount","designation"->"designation","updated_date"->"updated_date")

    val groupByCols = Seq("id","name").map(col)
    val aggregateCols = Seq("id","designation","updated_date")
    val aggExpr = aggregateCols.map(colName=>colOperationsMap(colName)(col(colName)).alias(colAliasMap(colName)))

    val result_group_by_df = df.orderBy(col("updated_date").desc)
                               .groupBy(groupByCols:_*)
                               .agg(aggExpr.head,aggExpr.tail:_*)
                               .filter(col("colCount")>0)
                               .drop("colCount")
    result_group_by_df.show()
  }
}
