package com.tmon.study.ch5

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by kimkyungmin on 2016-03-22.
  */

case class Person(name: String, lovesPandas: Boolean)

object JsonRead {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BasicJson")
    val sc = new SparkContext(conf)

    val inputFile = "files/pandainfo.json"
    val outputFile = "files/result"
    val input = sc.textFile(inputFile)

    //mapPartitions를 쓴 이유는 ObjectMapper가 serializable되지 않음으로, singleton으로 각 드라이버에 보낼 수 없다.
    //그래서 각 파티션별로 objectMapper를 생성하도록 했다. (비싼 연산이긴하다)
    val result = input.mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)

      //flatMap을 쓴 이유는 exception발생시 해당 내용을 무시(empty list, None)하기 위해서다.
      //정상적인 data는 Some(_)으로 쌓였다가 flat을 하면서 다시 벗겨지게 된다.
      records.flatMap(record => {
        try {
          Some(mapper.readValue(record, classOf[Person]))
        } catch {
          case e: Exception => None
        }
      })
    }, true)

    val outRDD = result.filter(_.lovesPandas).mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      records.map(mapper.writeValueAsString(_))
    })
    outRDD.saveAsTextFile(outputFile)
    println(outRDD.collect().mkString("\n"))

  }
}
