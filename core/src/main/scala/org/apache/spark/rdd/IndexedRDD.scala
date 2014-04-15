package org.apache.spark.rdd
import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}
import scala.util.Sorting
import collection.mutable.HashMap

private[spark] class IndexedRDD[K: ClassTag](prev: RDD[K]) 
    extends RDD[K](prev) {
   
  case class Pair(key:String, value: String)              

  object PairOrdering extends Ordering[Pair] {
      def compare(a: Pair, b: Pair) = a.key compare b.key
    }
       
  var self: RDD[String]
  
   def sortByKey():RDD[String] = {
      var array = this.map {case (key, value) => new Pair(key.toString(), value.toString())}.collect() // Collecting the elements of this RDD into an array 
      Sorting.quickSort(array)(PairOrdering) // Sorting the elements of this array
      self = this.context.parallelize(array).map(s => s.key) // Parallelize the given array and converts it into an RDD sorted by keys
      self
    }
   
   private var keyMap: HashMap[String, Int] = new HashMap[String, Int]() // Maps string to index 
   
   def buildIndex(): HashMap[String, Int] = {
     val sortRDD: RDD[String] = sortByKey()
     var index = 0
     sortRDD.foreach (s => {
    	 keyMap(s) = index 
    	 index += 1
       }
     )
     keyMap
   }
   
   def getMap: HashMap[String, Int] = {
     keyMap
   }
   
   override def getPartitions: Array[Partition] = self.partitions
    
   override def compute(split: Partition, context: TaskContext) =
    firstParent[K].iterator(split, context)
}