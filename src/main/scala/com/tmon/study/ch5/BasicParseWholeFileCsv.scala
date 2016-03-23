package com.tmon.study.ch5


import org.apache.spark._

/**
  * Created by kimkyungmin on 2016-03-22.
  */
object BasicParseWholeFileCsv {

  case class Person(name: String, favouriteAnimal: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BasicParseWholeFileCsv")
    val sc = new SparkContext(conf)

    val inputFile = "files/data"
    val outputFile = "files/result_whole_csv"
    val input = sc.wholeTextFiles(inputFile)

    println(input.collect().mkString("\n"))

   /* val result = input.flatMap{
      case (_, txt) =>
        val reader = new CSVReader(new StringReader(txt));
        reader.readAll()
    }*/
  }
}
