package org.basics

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object OrderJSONParse {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._
    val mySchema = StructType(
      List(
        StructField("filename",StringType)
        ,StructField("datasets",ArrayType(
            StructType(
              List(
                StructField("orderId",StringType)
                ,StructField("customerId",StringType)
                ,StructField("orderDate",StringType)
                ,StructField("shipmentDetails",StructType(
                    List(
                      StructField("street",StringType)
                      ,StructField("city",StringType)
                      ,StructField("state",StringType)
                      ,StructField("postalCode",StringType)
                      ,StructField("country",StringType)
                    )
                  )
                )
                ,StructField("orderDetails",ArrayType(
                  StructType(
                    List(
                      StructField("productId",StringType)
                      ,StructField("quantity",StringType)
                      ,StructField("sequence",StringType)
                      ,StructField("totalPrice",StructType(
                          List(
                            StructField("gross",IntegerType)
                            ,StructField("net",IntegerType)
                            ,StructField("tax",IntegerType)
                          )
                        )
                      )
                    )
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
      .option("multiLine",true)
      .schema(mySchema)
      .load("src/main/resources/datasets/orders.json")

    val result_df = df.drop("filename")
      .withColumn("order_data",explode(col("datasets")))
      .drop("datasets")
      .select("order_data.*")
      .withColumn("orderDetails",explode(col("orderDetails")))
      .select($"orderId"
              ,$"customerId"
              ,$"orderDate"
              ,$"shipmentDetails.*"
              ,$"orderDetails.*"
              ,$"orderDetails.totalPrice.gross".as("total_price")
              ).drop("totalPrice","sequence")
    result_df.show()
  }

}
