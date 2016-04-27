package com.ibm.scala.course

import java.io.File

import org.apache.commons.io.FileUtils

/**
  * Created by tgoetz on 4/1/16.
  */
object WordCount {

  def main(args: Array[String]) {
    val inputDirName = args(0)
    val inputDir = new File(inputDirName)
    val files = inputDir.listFiles()
    val texts = files.map(
      (file: File) => {
        FileUtils.readFileToString(file)
      }
    )
    val charCount = texts.map(_.length).fold(0)(_ + _)

    val delimiterRegex = "[ \t\n\r\f,.:;?!\\[\\]'\"(){}]+|--"
    val tokenArrays = texts.map(
      (text) => {
        text.split(delimiterRegex).map(_.toLowerCase)
      }
    )

    val tokCount = tokenArrays.foldLeft(0)((n, array) => n + array.size)
    println(s"Token count: ${tokCount}")

    val tf = tokenArrays.map(
      (tokens) => {
        tokens.foldLeft(Map.empty[String, Int])(
          (wc, token) => {
            wc + (token -> (wc.getOrElse(token, 0) + 1))
          }
        )
      }
    )

    //    tf.take(3).foreach(println(_))

    val idf0 = tf.foldLeft(Map.empty[String, Int])(
      (df, tc) => {
        tc.foldLeft(df)(
          (df, wcPair) => {
            wcPair match {
              case (token, _) => df + (token -> (df.getOrElse(token, 0) + 1))
            }
          }
        )
      }
    )

    val idf = idf0.map(
      { case (key, value) => key -> Math.log(1 + (texts.size.toDouble / value)) }
    )

    println(s"specific ${idf0.get("specific").get}")
    println(s"specific ${idf.get("specific").get}")
    println(s"incident ${idf0.get("incident").get}")
    println(s"incident ${idf.get("incident").get}")
    println(s"the ${idf0.get("the").get}")
    println(s"the ${idf.get("the").get}")


    println(idf.take(100))

    val tfIdf = tf.map(
      (wordCounts) => {
        wordCounts.foldLeft(Map.empty[String, Double])(
          (weightedCount, wc) => {
            wc match {
              case (key, value) => weightedCount + (key -> (value.toDouble * idf.getOrElse(key, 1.0)))
            }
          }
        )
      }
    )

    println(s"Number of maps: ${tf.size}")

    tfIdf.take(10).foreach(println(_))


    //    val wordCount = tokens.foldLeft(Map.empty[String, Int])(
    //      (wc, token) => {
    //        wc + (token -> (wc.getOrElse(token, 0) + 1))
    //      }
    //    )

    //    wordCount.filter( (pair) => pair._2 > 100).foreach((pair) => println(s"Word: ${pair._1}, count: ${pair._2}"))

    //    val topCount = wordCount.fold(("" -> 0))((accPair, wcPair) => {
    //      if (accPair._2 > wcPair._2) accPair else wcPair
    //    })
    //    println(s"Word '${topCount._1}' occurred ${topCount._2} times.")

    //    println(s"Number of files: ${files.length}, number of characters: ${charCount}, number of tokens: ${tokens
    // .length}")
  }

}
