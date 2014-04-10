package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext


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
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)
    val iters = args(2).toInt
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    val endTime = System.nanoTime
    val totalTime = (endTime - startTime)/1e6
    println("totalTime:" + totalTime + ":links_count:" + lines.count())
//    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    System.exit(0)
  }
}

