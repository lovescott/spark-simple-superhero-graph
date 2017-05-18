

package me.scottlove.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object SimplePopularSuperhero {

  def countCoOccurences(line: String): (Int, Int) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  def parseNames(line: String) : Option[(Int, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      Some(fields(0).trim().toInt, fields(1))
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularSuperhero")

    //Parse Superhero names
    val names = sc.textFile("marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)

    //Map co-occurences
    val graph = sc.textFile("marvel-graph.txt")
    val pairings = graph.map(countCoOccurences)

    val totalFriendsByCharacter = pairings.reduceByKey(_ + _)

    val flipped = totalFriendsByCharacter.map(_.swap)

    val mostPopular = flipped.max()

    val mostPopularName = namesRdd.lookup(mostPopular._2)(0)

    println(s"$mostPopularName is the most popular supperhero with ${mostPopular._1} co-appearances.")


  }


}