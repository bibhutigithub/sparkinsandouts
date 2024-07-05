package org.scenarios
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EmptyHeaderRddExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val sourcedf = spark.read.text("src/main/resources/datasets/emp_empty_headers.csv")
    val sourceRdd = sourcedf.rdd.zipWithIndex().filter(row=>row._2 > 1).map(row=>row._1)
    val headerRow = sourceRdd.first()
    val dataRowRdd = sourceRdd.filter(row=>row!=headerRow).map{ case(row)=>{
                                val splittedVals = row(0).toString.split(",")
                                (splittedVals(0),splittedVals(1),splittedVals(2)
                                  ,splittedVals(3),splittedVals(4),splittedVals(5)
                                  ,splittedVals(6),splittedVals(7),splittedVals(8))
                              }}
    val colList = headerRow(0).toString.split(",")
    val resultDF = dataRowRdd.collect.toSeq.toDF(colList:_*)
    resultDF.show()
    val originalColList = Seq("value")
    val sampleColList = Seq("id","name")
    val dataSeq = Seq(("100,Hi"),("200,Hello"))
    val df = dataSeq.toDF(originalColList:_*)
    var result_df = df.withColumn("value",split(col("value"),","))
    // One way to do
    var final_df = sampleColList.zipWithIndex.foldLeft(result_df) {
      case(df,colName) => df.withColumn(colName._1,col("value")(colName._2))
    }
    // rdd way
    val mySchema = StructType(List(
      StructField("id",IntegerType)
      ,StructField("name",StringType)
    ))
    val dataRdd1 = df.rdd.map{
      case(row) => {
        val splittedRows = row(0).toString.split(",")
        (splittedRows(0).toInt,splittedRows(1))
      }
    }

    val dataRdd2 = df.rdd.map{
      case(row) => {
        val splittedRows = row(0).toString.split(",")
        Row(splittedRows(0).toInt,splittedRows(1))
      }
    }
    val empDataSeq = dataRdd1.collect().toSeq
    val final_res_df = empDataSeq.toDF(sampleColList:_*)
    val final_res_df2 = spark.createDataFrame(dataRdd2,mySchema)
    final_res_df.show()
    final_res_df2.show()
  }
}
