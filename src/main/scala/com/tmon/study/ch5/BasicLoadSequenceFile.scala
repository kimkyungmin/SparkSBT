package com.tmon.study.ch5

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by kimkyungmin on 2016-03-24.
  */
object BasicLoadSequenceFile {



  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BasicLoadSequenceFile")
    val sc = new SparkContext(conf)

    val outputFile = "files/sequnce_file"
    val outData = sc.parallelize(List(("panda", 3), ("kay", 6), ("snail", 2)))
    outData.saveAsSequenceFile(outputFile)

    val inFile = "files/sequnce_file/part-00000"
    val data = sc.sequenceFile(inFile, classOf[Text], classOf[IntWritable])
        .map{
            case (x, y) => (x.toString, y.get())
        }
    println(data.collect().toList)
  }
}
