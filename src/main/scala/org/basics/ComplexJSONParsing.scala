package org.basics

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object ComplexJSONParsing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val mySchema = StructType(
      List(
        StructField("id",StringType)
        ,StructField("type",StringType)
        ,StructField("name",StringType)
        ,StructField("ppu",DoubleType)
        ,StructField("batters",StructType(
          List(
            StructField("batter",ArrayType(
              StructType(
                List(
                   StructField("id",StringType)
                  ,StructField("type",StringType)
                )
              )
            ))
          )
        ))
        ,StructField("topping",ArrayType(
          StructType(
            List(
              StructField("id",StringType)
              ,StructField("type",StringType)
            )
        ))
      )
    ))
    val df = spark.read
      .format("json")
      .option("multiLine",true)
      .schema(mySchema)
      .load("src/main/resources/datasets/donut_nested.json")

    val result_df = df.select("*","batters.*")
                    .drop("batters")
                    .withColumn("batter",explode(col("batter")))
                    .withColumn("batter_id",col("batter.id"))
                    .withColumn("batter_type",col("batter.type"))
                    .drop("batter")
                    .withColumn("topping",explode(col("topping")))
                    .withColumn("topping_id",col("topping.id"))
                    .withColumn("topping_type",col("topping.type"))
                    .drop("topping")

    print(result_df.count())
  }
}
