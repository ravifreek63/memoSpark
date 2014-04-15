package org.apache.spark.rdd
import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}
import scala.util.Sorting
import collection.mutable.HashMap

private[spark] class IndexedRDD[K: ClassTag](prev: RDD[K]) 
    extends RDD[K](prev) {
   
  case class Pair(key:Any, value:Any)              

  object PairOrdering extends Ordering[Pair] {
      def compare(a: Pair, b: Pair) = a.key.toString() compare b.key.toString()
    }
       
  var self: RDD[(String, String)] = sortByKey()
  
   def sortByKey():RDD[(String, String)] = {
      var array = this.map {case (key, value) => new Pair(key, value)}.collect() // Collecting the elements of this RDD into an array 
      Sorting.quickSort(array)(PairOrdering) // Sorting the elements of this array
      this.context.parallelize(array).map(s => (s.key.toString(), s.value.toString())) // Parallelize the given array and converts it into an RDD sorted by keys
    }
   
   private var keyMap: HashMap[String, Int] = new HashMap[String, Int]() // Maps string to index 
   
   def buildIndex(): HashMap[String, Int] = {
     val sortRDD: RDD[(String, String)] = sortByKey()
     var index = 0
     sortRDD.foreach (s => {
    	 keyMap(s._1) = index 
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