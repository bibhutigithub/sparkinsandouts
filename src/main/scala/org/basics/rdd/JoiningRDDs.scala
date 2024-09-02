package org.basics.rdd

import org.apache.spark.sql.SparkSession

object JoiningRDDs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    val leftDataSeq = Seq((100,"Bibhuti"),(200,"Soumya"),(300,"Plaban"), (400,"Prashant"))
    val rightDataSeq = Seq((100,"Physics"),(200,"Chemistry"),(300,"Math"),(500,"Biology"))

    val leftDataRdd = spark.sparkContext.parallelize(leftDataSeq)
    val rightDataRdd = spark.sparkContext.parallelize(rightDataSeq)

    println("====Applying Inner Join====")
    // Inner Join RDD
    val joinedRdd = leftDataRdd.join(rightDataRdd)
    joinedRdd.collect.foreach(println)

    println("====Applying Left Join====")
    // Left Outer Join RDD
    val leftJoinedRdd = leftDataRdd.leftOuterJoin(rightDataRdd)
    leftJoinedRdd.collect.foreach(println)

    println("====Applying Right Join====")
    // Right Outer Join RDD
    val rightJoinedRdd = leftDataRdd.rightOuterJoin(rightDataRdd)
    rightJoinedRdd.collect.foreach(println)
  }
}
