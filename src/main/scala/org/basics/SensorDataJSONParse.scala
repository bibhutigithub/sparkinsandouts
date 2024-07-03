package org.basics

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object SensorDataJSONParse {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val mySchema = StructType(
      List(
        StructField("dc_id",StringType)
        ,StructField("source",MapType(StringType,StructType(
          List(
            StructField("id",IntegerType)
            ,StructField("ip",StringType)
            ,StructField("description",StringType)
            ,StructField("temp",IntegerType)
            ,StructField("c02_level",IntegerType)
            ,StructField("geo",StructType(
              List(
                StructField("lat",DoubleType)
                ,StructField("long",DoubleType)
              )
            )
            )
          )
        )
        )
        )
      )
    )

    val df = spark.read
      .format("json")
      .option("multiLine",value = true)
      .schema(mySchema)
      .load("src/main/resources/datasets/sensors.json")

    val result_df = df
                    .select($"dc_id",explode($"source"))
                    .withColumnRenamed("key","source")
                    .select($"dc_id",$"source",$"value.*")
                    .withColumn("geo_lat",col("geo.lat"))
                    .withColumn("geo_long",col("geo.long"))
                    .drop("geo")
    result_df.show()
  }
}
