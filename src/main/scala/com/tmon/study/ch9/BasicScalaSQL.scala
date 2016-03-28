package com.tmon.study.ch9

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

case class HappyPerson(handle: String, favouriteBeverage: String)

/**
  * Created by kimkyungmin on 2016-03-25.
  */
object BasicScalaSQL {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BasicScalaSQL")
    conf.set("spark.sql.codegen", "false")
    conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "200")
    val sc = new SparkContext(conf)

    val hiveCtx = new HiveContext(sc)

    import hiveCtx.implicits._                      //import가 왜 여기 있는 거지?? 위로 올리면 에러 나넹????
    val happyPeopleRDD = sc.parallelize(List(HappyPerson("holden", "coffee"))).toDF()
    happyPeopleRDD.printSchema()
    happyPeopleRDD.registerTempTable("happy_people")
    println(happyPeopleRDD.collect().mkString("\n"))      //[holden,coffee]

    //- tweet ---------------------
    val inputFile: String = "files/testweet.json"
    val input = hiveCtx.jsonFile(inputFile)
    input.printSchema()
    input.registerTempTable("tweets")
    hiveCtx.cacheTable("tweets")

    val topTweets = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
    topTweets.collect().map(println(_))     //[Adventures With Coffee, Code, and Writing.,0]

    val topTweetText = topTweets.map(row => row.getString(0))
    println(topTweetText.collect().mkString("\n"))    //Adventures With Coffee, Code, and Writing.

    //--------  UDF
    hiveCtx.udf.register("strLenScala", (_: String).length)
    val tweetLength = hiveCtx.sql("SELECT strLenScala('tweet') FROM tweets LIMIT 10")
    tweetLength.collect().map(println(_))     //5

    val twoSums = hiveCtx.sql("SELECT SUM(user.favouritesCount), SUM(retweetCount), user.id FROM tweets GROUP BY user.id LIMIT 10")
    twoSums.collect().map(println(_))    //[1095,0,15594928]

    sc.stop()
  }

}
