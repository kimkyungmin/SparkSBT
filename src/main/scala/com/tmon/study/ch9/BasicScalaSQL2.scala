package com.tmon.study.ch9

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

case class Data(name: String, Age: Int)

/**
  * Created by kimkyungmin on 2016-03-25.
  */
object BasicScalaSQL2 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BasicScalaSQL")
    conf.set("spark.sql.codegen", "false")
    conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "200")
    val sc = new SparkContext(conf)

    val hiveCtx = new HiveContext(sc)

    val inputFile: String = "files/sample.json"
    val input = hiveCtx.jsonFile(inputFile)
    input.printSchema()
    input.registerTempTable("sample")
    hiveCtx.cacheTable("sample")

    val df = hiveCtx.sql("SELECT * FROM sample")
    df.collect().map(println(_))

    df.show()
    /*
    +----+----------+
    | age|      name|
    +----+----------+
    |null|      Bear|
    |   1|DataBricks|
    +----+----------+
     */

    val nameVal : DataFrame = df.select("name")
    println(nameVal.collect().mkString("\n"))
    //  [Bear]
    //  [DataBricks]

    val ageVal : DataFrame = df.select("name", "age")
    println(ageVal.collect().mkString("\n"))
    //  [Bear,null]
    //  [DataBricks,1]

    //val ageVal2 : DataFrame = df.select("name", df("age") + 1)   //이거는 컴파일 오류인데... ㅡㅡ
    //val filterVal : DataFrame = df.filter("age" > 1)   // 컴파일 오류

    sc.stop()
  }

}
