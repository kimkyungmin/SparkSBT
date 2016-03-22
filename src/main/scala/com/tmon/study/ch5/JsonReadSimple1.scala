package com.tmon.study.ch5

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.{SparkConf, SparkContext}


object JsonReadSimple1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BasicJson")
    val sc = new SparkContext(conf)

    val inputFile = "files/pandainfo.json"
    val outputFile = "files/result"
    val input = sc.textFile(inputFile, 10)
    println("input's partition size :: " + input.partitions.size)

    val result = input.map(record => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      mapper.readValue[Person](record, classOf[Person])
    })
    println("Read Json===== ")
    println(result.collect().mkString("\n"))

    val outRDD = result.filter(_.lovesPandas).mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      records.map(mapper.writeValueAsString(_))
    })
    outRDD.saveAsTextFile(outputFile)
    println(outRDD.collect().mkString("\n"))

  }

}
