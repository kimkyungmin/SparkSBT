package com.tmon.study.ch5

import java.io.StringReader
import java.io.StringWriter
import java.util

import org.apache.spark._
import scala.collection.JavaConversions._

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter

/**
  * Created by kimkyungmin on 2016-03-22.
  */
object BasicParseCsv {

  case class Person(name: String, favouriteAnimal: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BasicParseCsv")
    val sc = new SparkContext(conf)

    val inputFile = "files/favourite_animals.csv"
    val outputFile = "files/result_csv"
    val input = sc.textFile(inputFile)

    val result = input.map{ line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();  //return type이 Array[String]
    }
    val people = result.map(x => Person(x(0), x(1)))
    println("is input RDD.................")
    println(people.collect().mkString("\n"))

    val pandaLovers = people.filter(person => person.favouriteAnimal == "panda")

    val outputRDD = pandaLovers.map(person => List(person.name, person.favouriteAnimal).toArray).mapPartitions{people =>
      val stringWriter = new StringWriter();
      val csvWriter = new CSVWriter(stringWriter);
      //var toList: List[Array[String]] = people.toList        //import scala.collection.JavaConversions._ 없으면 scala List 반환
      //var toList: util.List[Array[String]] = people.toList   //import scala.collection.JavaConversions._ 있으면 java List 반환
      csvWriter.writeAll(people.toList)
      Iterator(stringWriter.toString)
    }
    println("is output RDD.................")
    println(outputRDD.collect().mkString("\n"))

    outputRDD.saveAsTextFile(outputFile)
  }
}
