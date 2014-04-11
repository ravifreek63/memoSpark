package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeMap

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
object IPageRank {
  // function which returns the absolute value of a double 
  def abs(args:  Double): Double = { 
    if (args < 0) -(args);
    else args;
  }

  // function which filters the graph based on some change in the rank  
  def filterGraph(outP:TreeMap[String, Double], outPOld:TreeMap[String, Double]):HashSet[String] = {
      val jetSet = new HashSet[String]
      val fChange = 0.1
      outP foreach ((element) => 
      if (outPOld.get(element._1) != None){
          for (ratio1_alias <- outPOld.get(element._1)){
             var change = abs((ratio1_alias) - element._2)
          if (change/element._2 > fChange){ 
            jetSet += element._1 // stores the set of static nodes
          }
         }
      }) 
      jetSet
    }
  
 def main(args: Array[String]) {
   if (args.length < 3) {
     System.err.println("Usage: PageRank <master> <file> <number_of_iterations>")
     System.exit(1)
   }
  if (args.length >3)
    System.setProperty("spark.executor.memory", args(3))
  else 
    System.setProperty("spark.executor.memory", "1024m")
  val ctx = new SparkContext(args(0), "SparkPageRank",
      "/memex/tandon/memoSpark", List("/memex/tandon/memoSpark/inc_page_rank/target/scala-2.10/page-rank_2.10-1.0.jar"))
    val lines = ctx.textFile(args(1), 1)
    val startTime = System.nanoTime
    var links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)
    val iters = args(2).toInt
    var outP = new TreeMap[String, Double]()
    var outPOld = new TreeMap[String, Double]()
    var filterNodes = new HashSet[String]
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        var urlF = urls.filter(url => filterNodes.contains(url) || filterNodes.size == 0)
        val size = urlF.size         
        urlF.map(url => (url, rank / size))
      }
      
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      println("iter:"+i+",ranks-length:"+ranks.count())
         if (i == 1)
           outP = scala.collection.immutable.TreeMap(ranks.collect():_*)
         if(i%10 == 1)
          outPOld = outP
         if(i%10  == 0) { 
          outP = scala.collection.immutable.TreeMap(ranks.collect():_*)
          filterNodes = filterGraph (outP, outPOld)
          ranks = ranks.filter(node => filterNodes.contains(node._1))
       }
      }

    val endTime = System.nanoTime
    val totalTime = (endTime - startTime)/1e6
    println("totalTime:" + totalTime + ":links_count:" + lines.count())
//  output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    System.exit(0)
  }
}

