package org.scenarios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, lit, to_date, when}
import org.apache.spark.sql.types.StructField

object PartitionByMonthAndYear {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
              .format("csv")
              .option("header",true)
              .option("inferSchema",true)
              .load("src/main/resources/datasets/emp.csv")
    val integerColList = List("EMPNO","MGR","SAL","COMM","DEPTNO")
    val all_column_types = df.schema.map{(field:StructField)=>(field.name,field.dataType)}
    val target_col_types = all_column_types.map{
                              case(colName,dataType)=>
                                  if(integerColList.contains(colName)) {
                                    (colName.toLowerCase(),"integer")
                                  } else{
                                    (colName.toLowerCase(),"string")
                                  }
                            }
    val result_df = df.select(target_col_types.map{case(c,t)=>col(c).cast(t)}:_*)
                        .withColumn("hiredate",when(col("hiredate") === "null" or col("hiredate") === "NULL",lit(null))
                                              .otherwise(col("hiredate")))
                        .withColumn("updated_date",when(col("updated_date") === "null" or col("updated_date") === "NULL",lit(null))
                                              .otherwise(col("updated_date")))
                        .na.fill("31-12-9999",Seq("hiredate","updated_date"))
                        .withColumn("hiredate",to_date(col("hiredate"),"dd-mm-yyyy"))
                        .withColumn("updated_date",to_date(col("updated_date")))
                        .withColumn("hiring_month",date_format(col("hiredate"),"MM"))
                        .withColumn("hiring_year",date_format(col("hiredate"),"yyyy"))
    result_df.write
             .format("parquet")
              .mode("overwrite")
             .partitionBy("hiring_year","hiring_month")
             .save("src/main/resources/outputs/employee/")
  }
}
