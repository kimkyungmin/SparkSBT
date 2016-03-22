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

    val mapper = new ObjectMapper with ScalaObjectMapper

    val result = input.mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)

      records.flatMap(record => {
        try {
          Some(mapper.readValue(record, classOf[Person]))
        } catch {
          case e: Exception => None
        }
      })
    }, true)
    println(result.collect().mkString(","))

    val outRDD = result.filter(_.lovesPandas).mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      records.map(mapper.writeValueAsString(_))
    })
    outRDD.saveAsTextFile(outputFile)
    println(outRDD.collect().mkString(","))
  }
}
