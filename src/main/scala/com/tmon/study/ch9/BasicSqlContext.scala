package com.tmon.study.ch9

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

case class Data(name: String, Age: Int)

/**
  * Created by kimkyungmin on 2016-03-25.
  */
object BasicSqlContext {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BasicScalaSQL")
    conf.set("spark.sql.codegen", "false")
    conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "200")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val inputFile: String = "files/people.json"
    val df = sqlContext.read.json(inputFile)
    df.show()
/*
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
*/

    df.printSchema()
/*
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)
 */

    df.select("name").show()
/*
+-------+
|   name|
+-------+
|Michael|
|   Andy|
| Justin|
+-------+
 */

    df.select(df("name"), df("age") + 1).show()
/*
+-------+---------+
|   name|(age + 1)|
+-------+---------+
|Michael|     null|
|   Andy|       31|
| Justin|       20|
+-------+---------+
 */

    df.filter(df("age") > 21).show()
/*
+---+----+
|age|name|
+---+----+
| 30|Andy|
+---+----+
 */

    df.groupBy("age").count().show()
/*
+----+-----+
| age|count|
+----+-----+
|null|    1|
|  19|    1|
|  30|    1|
+----+-----+
 */
    sc.stop()
  }

}
