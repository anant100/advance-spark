// PopularSuperhero
package com.bigdata.spark.advance

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import math.max

object PopularSuperhero extends App {
  
  def SuperheroNames(line: String): Option[(Int, String)] = {
    
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    }
    else {
      return None
    }
  }
  
  def SuperheroGraph(line: String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length-1 )
  }
  
  // set log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  // Create SparkContext
  val sc = new SparkContext("local[*]", "PopularSuperhero")
  
  // Build RDD of heroId -> Name
  val names = sc.textFile("../SparkScala3/Marvel-names.txt")
  val namesRdd = names.flatMap(SuperheroNames)
  
  // Build RDD of Superhero Graph (HeroId, number of connections)
  val graph = sc.textFile("../SparkScala3/Marvel-graph.txt")
  val graphRdd = graph.map(SuperheroGraph)
  
  // Sumup duplicate values
  val totalFriends = graphRdd.reduceByKey( (x,y) =>  x + y )
  
  // Flip to (number of connections, HeroId) to use Key
  val flipGraph = totalFriends.map(x => (x._2, x._1) )
  
  // Get Max number from Key Which gives Most highed numbers of connections
  val mostPopular = flipGraph.max()
  
  // Lookup the max to NamesRDD to get name of Most Popular Superhero
  val mostPopularName = namesRdd.lookup(mostPopular._2)(0)
  println(s"Most Popular Superhero -> $mostPopularName \nTotal Connections -> ${mostPopular._1}")
  
  // Top 10 Most Popular Superhero
  val topMost = flipGraph.sortByKey(false).take(10)
  println("\n- Top 10 Most Popular Superhero -")
  val top10 = topMost.map(x => namesRdd.lookup(x._2)(0))
  top10.foreach(println)
}
