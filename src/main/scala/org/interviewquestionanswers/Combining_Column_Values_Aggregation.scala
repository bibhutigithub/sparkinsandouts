package org.interviewquestionanswers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_set

/**
 *    Input :-
 *    col_1     col_2      col_3
 *    -----     -----      ----
 *    a         aa          1
 *    a         aa          2
 *    b         bb          5
 *    b         bb          3
 *    b         bb          4
 *
 *    Output:-
 *    col_1     col_2      col_3
 *    -----     -----      ----
 *    a         aa          [1,2]
 *    b         bb          [5,3,4]
 *
 */

object Combining_Column_Values_Aggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .getOrCreate()

    import spark.implicits._

    val dataSeq = Seq(("a", "aa", 1)
                      , ("a", "aa", 2)
                      , ("b", "bb", 5)
                      , ("b", "bb", 3)
                      , ("b", "bb", 4))
    val df1 = dataSeq.toDF("col_1", "col_2","col_3")
    val result_df = df1.groupBy("col_1","col_2").agg(collect_set("col_3").alias("col_3"))
    result_df.show()
  }
}
