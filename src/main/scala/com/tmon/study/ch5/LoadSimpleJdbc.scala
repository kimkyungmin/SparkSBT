package com.tmon.study.ch5

import java.sql.{ResultSet, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by kimkyungmin on 2016-03-24.
  */
object LoadSimpleJdbc {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BasicParseCsv")
    val sc = new SparkContext(conf)

    val myRDD = new JdbcRDD( sc,
      createConnection,
      "SELECT deal_srl, title FROM deal WHERE deal_srl IN ( 268368573, 300531794, 297174505, 297070385) LIMIT ?, ?",
      lowerBound = 1, upperBound = 3,
      numPartitions = 2,
      mapRow = extractValues
    )

    myRDD.foreach(println)
    println("myRDD's partition size : " + myRDD.partitions.size)


  }

  def createConnection() = {
    val url = "jdbc:mysql://devdb.tmonc.net:3306/tmon_billing"
    val username = "TM_DDL2_COMUSER"
    val password = "TM_DDL2_COMUSER1234!!"

    Class.forName("com.mysql.jdbc.Driver").newInstance();
    DriverManager.getConnection(url,username,password);
  }

  def extractValues(r: ResultSet) = {
    (r.getInt(1), r.getString(2))
  }

}
