package com.ibm.scala.course

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tgoetz on 4/12/16.
  */
object WordCountSpark {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Word Count App").set("spark.cores.max", "8")
    val sc = new SparkContext(sparkConfig)

    val input = sc.wholeTextFiles((new File(args(0))).toURI.toString)

    val delimiterRegex = "[ \t\n\r\f,.:;?!\\[\\]'\"(){}]+|--"
    val tokens = input.map({case (_, value) => value}).map(_.split(delimiterRegex).map(_.toLowerCase))
    val tokCount = tokens.map(_.size).fold(0)({ _ + _ })

    println(s"Token count: ${tokCount}")
  }


}
