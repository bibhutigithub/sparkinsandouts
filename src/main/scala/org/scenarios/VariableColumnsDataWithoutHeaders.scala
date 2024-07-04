package org.scenarios

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField

object VariableColumnsDataWithoutHeaders {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val columnNameList = Seq("id","name","designation","salary")

    val df = spark.read.text("src/main/resources/datasets/emp_data_no_header.csv")
    val manual_column_rename_df = df.withColumn("value",split(col("value"),","))
      .withColumn("id",col("value")(0))
      .withColumn("name",col("value")(1))
      .withColumn("designation",col("value")(2))
      .withColumn("salary",col("value")(3))
      .drop("value")

    // Another approach to create dynamic columns

    var final_df = df.withColumn("value",split(col("value"),","))
    val max_columns_count = final_df.withColumn("max_columns",size(col("value")))
                            .select(max(col("max_columns"))).collect()(0)(0).toString.toInt

    for (i <-0 to max_columns_count-1){
      final_df = final_df.withColumn("col_"+i, col("value")(i))
    }

    val result_df = final_df.drop("value")

    // This is one approach
    val all_columns_renamed_df = final_df
      .drop("value")
      .toDF(columnNameList:_*)
    all_columns_renamed_df.show()

    //Another approach
    val final_result_df = columnNameList.foldLeft(result_df:DataFrame)(
      (df,newColName) => {
        df.withColumnRenamed(df.columns(columnNameList.indexOf(newColName)),newColName)
      }
    )
    final_result_df.show()
  }
}
