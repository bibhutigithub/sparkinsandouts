package org.basics.rdd

import org.apache.spark.sql.SparkSession

object RDDOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    val dataSeq = Seq("USD;Euro;GBP;CHF","CHF;JPY;CNY;KRW","CNY;KRW;Euro;USD","CAD;NZD;SEK;MXN")
    val dataRdd = spark.sparkContext.parallelize(dataSeq)
    val resultRdd = dataRdd.flatMap(data=>data.split(";"))
      .map(currency=>(currency,1))
      .reduceByKey(_+_)
    resultRdd.collect.foreach(println)
  }
}
