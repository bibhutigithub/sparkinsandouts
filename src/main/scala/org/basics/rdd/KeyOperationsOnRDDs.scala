package org.basics.rdd

import org.apache.spark.sql.SparkSession

object KeyOperationsOnRDDs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    //  Merging values with Neutral Zero values.
    val normalCollection = Seq(("a", 1), ("a", 3), ("b", 2), ("a", 5), ("b", 4), ("a", 7), ("b", 6))
    val normalRdd = spark.sparkContext.parallelize(normalCollection)
    val resultRdd = normalRdd.foldByKey(0)(_+_)
    //resultRdd.collect.foreach(println)

    // aggregateByKey Demonstration
    val student_rdd = spark.sparkContext.parallelize(Seq(
      ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91),
      ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
      ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78),
      ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
      ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91),
      ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65),
      ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86),
      ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
      ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64),
      ("Juan", "Biology", 60)), 3)

    val seqCombineOps = (elem1Tuple:(Double,Int),elem2Tuple:(Double,Int))=>{
      (elem1Tuple._1 + elem2Tuple._1, elem1Tuple._2 + elem2Tuple._2)
    }
    val aggTransformedRdd = student_rdd.map(elem=>(elem._1,(elem._3.toDouble,1)))
    val aggResultRdd = aggTransformedRdd.aggregateByKey(new Tuple2[Double,Int](0.0,0))(seqCombineOps,seqCombineOps)
      .map(elem=>(elem._1, elem._2._1/elem._2._2))
    //aggResultRdd.collect.foreach(println)

    // CombineByKey Demonstration
    val combinerOps = (value:(String,Int)) => {
      (value._2.toDouble,1)
    }

    val mergerOps = (accumulator:(Double,Int),elem:(String,Int)) => {
      (accumulator._1+elem._2,accumulator._2+1)
    }

    val mergeCombinerOps = (value1:(Double,Int),value2:(Double,Int)) => {
      (value1._1+value2._1,value1._2+value2._2)
    }

    val combTransformRdd = student_rdd.map(elem=>(elem._1,(elem._2,elem._3)))
    val combResultRdd = combTransformRdd.combineByKey(combinerOps,mergerOps,mergeCombinerOps)
    combResultRdd.collect.foreach(println)
  }
}
